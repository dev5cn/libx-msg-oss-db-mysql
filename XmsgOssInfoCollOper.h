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

#ifndef XMSGOSSINFOCOLLOPER_H_
#define XMSGOSSINFOCOLLOPER_H_

#include <libx-msg-oss-core.h>

class XmsgOssInfoCollOper
{
public:
	bool queryByGts(SptrCgt cgt, ullong sts, ullong ets, int page, int pageSize, list<shared_ptr<XmsgOssInfoColl>>& lis); 
public:
	bool load(bool (*loadCb)(shared_ptr<XmsgOssInfoColl> coll)); 
	shared_ptr<XmsgOssInfoColl> find(const string& oid); 
	bool insert(shared_ptr<XmsgOssInfoColl> coll); 
	bool updateStoreSize(const string& oid, ullong storeSize); 
	bool updateStoreSize(void* conn, const string& oid, ullong storeSize); 
	static XmsgOssInfoCollOper* instance();
private:
	static XmsgOssInfoCollOper* inst;
	shared_ptr<XmsgOssInfoColl> loadOneFromIter(void* it); 
	XmsgOssInfoCollOper();
	virtual ~XmsgOssInfoCollOper();
};

#endif 
