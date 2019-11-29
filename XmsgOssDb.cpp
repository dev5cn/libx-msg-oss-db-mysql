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
#include "XmsgOssDb.h"
#include "XmsgOssInfoCollOper.h"

XmsgOssDb* XmsgOssDb::inst = new XmsgOssDb();
string XmsgOssDb::xmsgOssCfgColl = "tb_x_msg_oss_cfg"; 
string XmsgOssDb::xmsgOssInfoColl = "tb_x_msg_oss_info"; 

XmsgOssDb::XmsgOssDb()
{

}

XmsgOssDb* XmsgOssDb::instance()
{
	return XmsgOssDb::inst;
}

bool XmsgOssDb::load()
{
	auto& cfg = XmsgOssCfg::instance()->cfgPb->mysql();
	if (!MysqlConnPool::instance()->init(cfg.host(), cfg.port(), cfg.db(), cfg.usr(), cfg.password(), cfg.poolsize()))
		return false;
	LOG_INFO("init mysql connection pool successful, host: %s:%d, db: %s", cfg.host().c_str(), cfg.port(), cfg.db().c_str())
	if ("mysql" == XmsgOssCfg::instance()->cfgPb->cfgtype() && !this->initCfg())
		return false;
	if (!XmsgOssInfoCollOper::instance()->load(XmsgOssInfoMgr::loadCb4objInfo))
		return false;
	this->abst.reset(new ActorBlockingSingleThread("obj-db")); 
	return true;
}

void XmsgOssDb::future(function<void()> cb)
{
	this->abst->future(cb);
}

bool XmsgOssDb::initCfg()
{
	return true;
}

XmsgOssDb::~XmsgOssDb()
{

}

