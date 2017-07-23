package com.tydic.unicom.crm.sysManager.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.tydic.uda.core.Condition;
import com.tydic.unicom.crm.sysManager.po.InfoRole;
import com.tydic.unicom.crm.sysManager.service.interfaces.RoleServ;
import com.tydic.unicom.vdsBase.po.Pageable;
import com.tydic.unicom.vdsBase.service.impl.BaseServImpl;
import com.tydic.unicom.webUtil.Page;

public class RoleServImpl extends BaseServImpl<InfoRole> implements RoleServ {

	/**
	 * 
	 * @author wanghao 2014年8月7日 上午10:13:24
	 * @Method: addInfoRole 
	 * @Description: TODO 新增角色
	 * @param infoRole
	 * @return true/false
	 * @see com.tydic.unicom.crm.sysManager.service.interfaces.RoleServ#addInfoRole(com.tydic.unicom.crm.sysManager.po.InfoRole)
	 */
	@Override
	public boolean addInfoRole(InfoRole infoRole) {
		create(InfoRole.class, infoRole);
		return true;
	}

	
	/**
	 * 
	 * @author wanghao 2014年8月7日 上午10:13:43
	 * @Method: deleteInfoRole 
	 * @Description: TODO 删除角色
	 * @param role_ids
	 * @return true/false
	 * @see com.tydic.unicom.crm.sysManager.service.interfaces.RoleServ#deleteInfoRole(java.lang.String)
	 */
	@Override
	public boolean deleteInfoRole(String role_ids) {
		Map<String, Object> filter = new HashMap<String, Object>();
		filter.put("role_id", role_ids);
		Condition con = Condition.build("queryByRoleIds").filter(filter);
		List<InfoRole> list = query(InfoRole.class, con);

		if (list.size() > 0)
			return false;
		int i = remove(InfoRole.class, role_ids);
		if (i > 0)
			return true;
		else
			return false;
	}

	
	/**
	 * 
	 * @author wanghao 2014年8月7日 上午10:14:20
	 * @Method: getInfoRoleById 
	 * @Description: TODO 通过角色ID查询
	 * @param role_id
	 * @return InfoRole
	 * @see com.tydic.unicom.crm.sysManager.service.interfaces.RoleServ#getInfoRoleById(java.lang.String)
	 */
	@Override
	public InfoRole getInfoRoleById(String role_id) {
		return (InfoRole) get(InfoRole.class, role_id);
	}

	
	/**
	 * 
	 * @author wanghao 2014年8月7日 上午10:14:34
	 * @Method: updateInfoRole 
	 * @Description: TODO 更新角色
	 * @param infoRole
	 * @return true/false
	 * @see com.tydic.unicom.crm.sysManager.service.interfaces.RoleServ#updateInfoRole(com.tydic.unicom.crm.sysManager.po.InfoRole)
	 */
	@Override
	public boolean updateInfoRole(InfoRole infoRole) {
		int i = update(InfoRole.class, infoRole);
		if (i > 0)
			return true;
		else
			return false;
	}

	
	/**
	 * 
	 * @author wanghao 2014年8月7日 上午10:14:46
	 * @Method: queryPageInfoRole 
	 * @Description: TODO 分页查询
	 * @param pageable
	 * @return Page<InfoRole>
	 * @see com.tydic.unicom.crm.sysManager.service.interfaces.RoleServ#queryPageInfoRole(com.tydic.unicom.vdsBase.po.Pageable)
	 */
	@Override
	public Page<InfoRole> queryPageInfoRole(Pageable<InfoRole> pageable) {

	    return page(InfoRole.class, pageable, "queryPageInfoRole", "queryCountInfoRole");
	}

	/**
	 * 
	 * @author wanghao 2014年8月7日 上午10:15:00
	 * @Method: queryCountInfoRole 
	 * @Description: TODO 分页总数
	 * @param pageable
	 * @return Integer
	 * @see com.tydic.unicom.crm.sysManager.service.interfaces.RoleServ#queryCountInfoRole(com.tydic.unicom.vdsBase.po.Pageable)
	 */
	@Override
	public Integer queryCountInfoRole(Pageable<InfoRole> pageable){
		return count(pageable, "queryCountInfoRole");
	}
}
