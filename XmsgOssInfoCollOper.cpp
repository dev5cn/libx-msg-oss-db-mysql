/*
  Copyright 2019 www.dev5.cn, Inc. dev5@qq.com
 
  This file is part of X-MSG-IM.
 
  X-MSG-IM is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  X-MSG-IM is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.
 
  You should have received a copy of the GNU Affero General Public License
  along with X-MSG-IM.  If not, see <https://www.gnu.org/licenses/>.
 */

#include <libmisc-mysql-c.h>
#include "XmsgOssInfoCollOper.h"
#include "XmsgOssDb.h"

XmsgOssInfoCollOper* XmsgOssInfoCollOper::inst = new XmsgOssInfoCollOper();

XmsgOssInfoCollOper::XmsgOssInfoCollOper()
{

}

XmsgOssInfoCollOper* XmsgOssInfoCollOper::instance()
{
	return XmsgOssInfoCollOper::inst;
}

bool XmsgOssInfoCollOper::load(bool (*loadCb)(shared_ptr<XmsgOssInfoColl> coll))
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool.")
		return false;
	}
	ullong now = Xsc::clock;
	now -= XmsgOssCfg::instance()->cfgPb->misc().objinfocached() * DateMisc::day;
	string sql;
	SPRINTF_STRING(&sql, "select * from %s where gts > '%s'", XmsgOssDb::xmsgOssInfoColl.c_str(), DateMisc::to_yyyy_mm_dd_hh_mi_ss_ms(now).c_str())
	int size = 0;
	bool ret = MysqlMisc::query(conn, sql, [loadCb, &size](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgOssDb::xmsgOssInfoColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgOssDb::xmsgOssInfoColl.c_str())
			return true;
		}
		auto coll = XmsgOssInfoCollOper::instance()->loadOneFromIter(row.get());
		if (coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgOssDb::xmsgOssInfoColl.c_str(), row->toString().c_str())
			return false; 
		}
		LOG_RECORD("got a %s: %s", XmsgOssDb::xmsgOssInfoColl.c_str(), coll->toString().c_str())
		loadCb(coll);
		++size;
		return true;
	});
	LOG_DEBUG("load %s.%s successful, size: %d", MysqlConnPool::instance()->getDbName().c_str(), XmsgOssDb::xmsgOssInfoColl.c_str(), size)
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

shared_ptr<XmsgOssInfoColl> XmsgOssInfoCollOper::find(const string& oid)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, oid: %s", oid.c_str())
		return nullptr;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s where oid = ?", XmsgOssDb::xmsgOssInfoColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow()->addVarchar(oid);
	shared_ptr<XmsgOssInfoColl> coll;
	bool ret = MysqlMisc::query(conn, req, [&coll](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("load %s failed, ret: %d, desc: %s", XmsgOssDb::xmsgOssInfoColl.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("table %s no record", XmsgOssDb::xmsgOssInfoColl.c_str())
			return true;
		}
		coll = XmsgOssInfoCollOper::instance()->loadOneFromIter(row.get());
		if (coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgOssDb::xmsgOssInfoColl.c_str(), row->toString().c_str())
			return false; 
		}
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return coll;
}

bool XmsgOssInfoCollOper::insert(shared_ptr<XmsgOssInfoColl> coll)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, coll: %s", coll->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "insert into %s values (?, ?, ?, ?, ?, ?, ?, ?, ?)", XmsgOssDb::xmsgOssInfoColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(coll->oid) 
	->addVarchar(coll->cgt->toString()) 
	->addVarchar(coll->hashVal) 
	->addVarchar(coll->objName) 
	->addVarchar(coll->storePath) 
	->addLong(coll->objSize) 
	->addLong(coll->storeSize) 
	->addBlob(coll->info->SerializeAsString()) 
	->addDateTime(coll->gts);
	bool ret = MysqlMisc::sql(conn, req, [coll](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("insert %s.%s failed, coll: %s, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgOssDb::xmsgOssInfoColl.c_str(), coll->toString().c_str(), desc.c_str())
			return;
		}
		LOG_TRACE("insert %s.%s successful, coll: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgOssDb::xmsgOssInfoColl.c_str(), coll->toString().c_str())
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgOssInfoCollOper::updateStoreSize(const string& oid, ullong storeSize)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, oid: %s", oid.c_str())
		return false;
	}
	bool ret = this->updateStoreSize(conn, oid, storeSize);
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

bool XmsgOssInfoCollOper::updateStoreSize(void* conn, const string& oid, ullong storeSize)
{
	string sql;
	SPRINTF_STRING(&sql, "update %s set storeSize = %llu where oid = '%s'", XmsgOssDb::xmsgOssInfoColl.c_str(), storeSize, oid.c_str())
	return MysqlMisc::sql((MYSQL*) conn, sql, [sql](int ret, const string& desc, int effected)
	{
		if (ret != 0)
		{
			LOG_ERROR("update %s.%s failed, sql: %s, error: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgOssDb::xmsgOssInfoColl.c_str(), sql.c_str(), desc.c_str())
			return;
		}
		LOG_TRACE("update %s.%s successful, sql: %s", MysqlConnPool::instance()->getDbName().c_str(), XmsgOssDb::xmsgOssInfoColl.c_str(), sql.c_str())
	});
}

bool XmsgOssInfoCollOper::queryByGts(SptrCgt cgt, ullong sts, ullong ets, int page, int pageSize, list<shared_ptr<XmsgOssInfoColl>>& lis)
{
	MYSQL* conn = MysqlConnPool::instance()->getConn();
	if (conn == NULL)
	{
		LOG_ERROR("can not get connection from pool, oid: %s", cgt->toString().c_str())
		return false;
	}
	string sql;
	SPRINTF_STRING(&sql, "select * from %s where cgt = ? and (gts >= ? and gts < ?) order by gts asc limit ?, ?", XmsgOssDb::xmsgOssInfoColl.c_str())
	shared_ptr<MysqlCrudReq> req(new MysqlCrudReq(sql));
	req->addRow() 
	->addVarchar(cgt->toString()) 
	->addDateTime(sts) 
	->addDateTime(ets) 
	->addLong(page * pageSize) 
	->addLong(pageSize);
	bool ret = MysqlMisc::query(conn, req, [sql, &lis](int ret, const string& desc, bool more, int rows, shared_ptr<MysqlResultRow> row)
	{
		if (ret != 0) 
		{
			LOG_ERROR("execute sql %s failed, ret: %d, desc: %s", sql.c_str(), ret, desc.c_str())
			return false;
		}
		if (row == NULL) 
		{
			LOG_DEBUG("execute sql %s no record", sql.c_str())
			return true;
		}
		shared_ptr<XmsgOssInfoColl> coll = XmsgOssInfoCollOper::instance()->loadOneFromIter(row.get());
		if (coll == nullptr)
		{
			LOG_ERROR("have some one %s format error, row: %s", XmsgOssDb::xmsgOssInfoColl.c_str(), row->toString().c_str())
			return false; 
		}
		lis.push_back(coll);
		return true;
	});
	MysqlConnPool::instance()->relConn(conn, ret);
	return ret;
}

shared_ptr<XmsgOssInfoColl> XmsgOssInfoCollOper::loadOneFromIter(void* it)
{
	MysqlResultRow* row = (MysqlResultRow*) it;
	string oid;
	if (!row->getStr("oid", oid))
	{
		LOG_ERROR("can not found field: oid")
		return nullptr;
	}
	string str;
	if (!row->getStr("cgt", str))
	{
		LOG_ERROR("can not found field: cgt")
		return nullptr;
	}
	SptrCgt cgt = ChannelGlobalTitle::parse(str);
	if (cgt == nullptr)
	{
		LOG_ERROR("mcgt format error: %s", str.c_str())
		return nullptr;
	}
	string hashVal;
	if (!row->getStr("hashVal", hashVal))
	{
		LOG_ERROR("can not found field: hashVal")
		return nullptr;
	}
	string objName;
	if (!row->getStr("objName", objName))
	{
		LOG_ERROR("can not found field: objName")
		return nullptr;
	}
	string storePath;
	if (!row->getStr("storePath", storePath))
	{
		LOG_ERROR("can not found field: storePath")
		return nullptr;
	}
	ullong objSize;
	if (!row->getLong("objSize", objSize))
	{
		LOG_ERROR("can not found field: objSize")
		return nullptr;
	}
	ullong storeSize;
	if (!row->getLong("storeSize", storeSize))
	{
		LOG_ERROR("can not found field: storeSize")
		return nullptr;
	}
	if (!row->getBin("info", str))
	{
		LOG_ERROR("can not found field: info")
		return nullptr;
	}
	shared_ptr<XmsgKv> info(new XmsgKv());
	if (!info->ParseFromString(str))
	{
		LOG_ERROR("XmsgKv format error: %s", cgt->toString().c_str())
		return nullptr;
	}
	ullong gts;
	if (!row->getLong("gts", gts))
	{
		LOG_ERROR("can not found field: gts, cgt: %s", cgt->toString().c_str())
		return nullptr;
	}
	shared_ptr<XmsgOssInfoColl> coll(new XmsgOssInfoColl());
	coll->oid = oid;
	coll->cgt = cgt;
	coll->hashVal = hashVal;
	coll->objName = objName;
	coll->storePath = storePath;
	coll->objSize = objSize;
	coll->storeSize = storeSize;
	coll->info = info;
	coll->gts = gts;
	return coll;
}

XmsgOssInfoCollOper::~XmsgOssInfoCollOper()
{

}

