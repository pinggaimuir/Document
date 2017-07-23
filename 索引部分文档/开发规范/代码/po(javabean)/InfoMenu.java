package com.tydic.unicom.crm.sysManager.po;

import java.util.Date;

import com.tydic.uda.core.Sort;
import com.tydic.unicom.vdsBase.po.BasePo;
import com.tydic.unicom.vdsBase.annotation.*;;

public class InfoMenu extends BasePo{
	private static final long serialVersionUID = 1L;
	
	private String menu_id;
	private String authority_id;
	
	@like
	private String menu_name;
	@like
	private String menu_url;
	@like
	private String menu_comment;
	@sort(order = Sort.DESC)
	private Date  create_date;
	private String  create_operator_id;
	
	private String  up_menu_id;
	
	public String getMenu_id() {
		return menu_id;
	}
	public void setMenu_id(String menu_id) {
		this.menu_id = menu_id;
	}
	public String getAuthority_id() {
		return authority_id;
	}
	public void setAuthority_id(String authority_id) {
		this.authority_id = authority_id;
	}
	public String getMenu_name() {
		return menu_name;
	}
	public void setMenu_name(String menu_name) {
		this.menu_name = menu_name;
	}
	public String getMenu_url() {
		return menu_url;
	}
	public void setMenu_url(String menu_url) {
		this.menu_url = menu_url;
	}
	public String getMenu_comment() {
		return menu_comment;
	}
	public void setMenu_comment(String menu_comment) {
		this.menu_comment = menu_comment;
	}
	public Date getCreate_date() {
		return create_date;
	}
	public void setCreate_date(Date create_date) {
		this.create_date = create_date;
	}
	public String getCreate_operator_id() {
		return create_operator_id;
	}
	public void setCreate_operator_id(String create_operator_id) {
		this.create_operator_id = create_operator_id;
	}
	public String getUp_menu_id() {
		return up_menu_id;
	}
	public void setUp_menu_id(String up_menu_id) {
		this.up_menu_id = up_menu_id;
	}
	
	
}
