/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package dao;

import java.util.*;

import models.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.EmptyResultDataAccessException;
import play.Logger;

public class UserDAO extends AbstractMySQLOpenSourceDAO
{
	private final static String GET_CURRENT_USER_INFO  = "SELECT u.id, u.name, u.email, " +
			"u.username, u.department_number, s.detail_default_view, s.default_watch " +
			"FROM users u LEFT JOIN user_settings s on u.id = s.user_id WHERE username = ?";

	private final static String UPDATE_USER_SETTINGS = "INSERT INTO user_settings" +
			"(user_id, detail_default_view, default_watch) VALUES(?, ?, ?) " +
			"ON DUPLICATE KEY UPDATE detail_default_view = ?, default_watch = ?";

	private final static String GET_USER_ID = "SELECT id FROM users WHERE username = ?";

	private final static String CREATE_USER = "INSERT INTO users " +
			"(name, username, password_digest, email, password_digest_type, authentication_type) " +
			"VALUES(?, ?, SHA1(?), ? , 'SHA1', 'default')";

	private final static String CREATE_LDAP_USER =
			"INSERT INTO users (name, username, email, department_number, authentication_type) VALUES(?, ?, ?, ?, 'LDAP')";

	private final static String GET_USER_COUNT = "SELECT COUNT(*) FROM users WHERE username = ?";

	private final static String GET_USER_INFO_BY_USERNAME = "SELECT id, password_digest, " +
			"authentication_type FROM users WHERE username = ? and authentication_type = 'default'";

	private final static String GET_ALL_COMPANY_USERS = "SELECT DISTINCT user_id as id, display_name as name " +
			"FROM dir_external_user_info";

	private final static String GET_ALL_GROUPS = "SELECT DISTINCT group_id as name FROM dir_external_group_user_map " +
			"WHERE group_id is not null and group_id != ''";

	private final static String GET_ALL_COMPANY_USERS_AND_GROUPS = "SELECT DISTINCT user_id as id, " +
			"display_name as name, 'indiviual person' as category FROM dir_external_user_info " +
			"UNION SELECT DISTINCT group_id as id, NULL as name, 'group' as category FROM dir_external_group_user_map";

	private final static String INSERT_USER_LOGIN_HISTORY =
			"INSERT INTO user_login_history (username, authentication_type, `status`, message) VALUES (?, ?, ?, ?)";

	private final static String PASSWORD_COLUMN = "password_digest";

	private final static String DEFAULT_DETAIL_VIEW = "accordion";

	private final static String DEFAULT_WATCH = "weekly";

	public static String signUp(String userName, String firstName, String lastName, String email, String password)
	{
		String message = "Sign up failed. Please try again.";
		if (StringUtils.isBlank(userName))
		{
			return "User name is required.";
		}

		if (StringUtils.isBlank(firstName))
		{
			return "First name is required.";
		}

		if (StringUtils.isBlank(lastName))
		{
			return "Last name is required.";
		}

		if (StringUtils.isBlank(password))
		{
			return "Password is required and must be at least 6 characters.";
		}

		if (userExist(userName))
		{
			message = "The username you input has been used. Please choose another.";
		}
		else
		{
			int row = getJdbcTemplate().update(CREATE_USER,
					firstName + " " + lastName,
					userName,
					password,
					email);
			if (row > 0)
			{
				message = "";
			}
		}
		return message;
	}

	public static void addLdapUser(User user)
	{
		if (user != null && StringUtils.isNotBlank(user.userName))
		{
			getJdbcTemplate().update(CREATE_LDAP_USER,
				user.name,
				user.userName,
				user.email,
				user.departmentNum);
		}
	}

	public static Boolean userExist(String userName)
	{
		if (StringUtils.isNotBlank(userName))
		{
			Integer count = (Integer)getJdbcTemplate().queryForObject(
					GET_USER_COUNT,
					Integer.class,
					userName);
			if (count != null && count > 0)
			{
				return true;
			}
		}
		return false;
	}

	public static Boolean authenticate(String userName, String password)
	{
		List<Map<String, Object>> rows = null;
		rows = getJdbcTemplate().queryForList(
				GET_USER_INFO_BY_USERNAME,
				userName);
		if (rows != null)
		{
			for (Map row : rows) {

				String digestPassword = (String)row.get(PASSWORD_COLUMN);
				if (StringUtils.isNotBlank(digestPassword))
				{
					if (digestPassword.equals(play.api.libs.Codecs.sha1(password)))
					{
						return true;
					}
				}
			}
		}
		return false;
	}

	public static User getCurrentUser(String username)
	{
		User user = new User();
		try
		{
			if (StringUtils.isNotBlank(username))
			{
				user = (User)getJdbcTemplate().queryForObject(
						GET_CURRENT_USER_INFO,
						new UserRowMapper(),
						username);
			}
		}
		catch(EmptyResultDataAccessException e)
		{
			Logger.error("UserDAO getCurrentUser failed, username = " + username);
			Logger.error("Exception = " + e.getMessage());
		}

		return user;
	}

	public static Integer getUserIDByUserName(String userName)
	{
		Integer userId = 0;
		if (StringUtils.isNotBlank(userName))
		{
			try {
				userId = (Integer)getJdbcTemplate().queryForObject(
						GET_USER_ID,
						Integer.class,
						userName);
			} catch (EmptyResultDataAccessException e) {
				userId = 0;
				Logger.error("Get user id failed, user name = " + userName);
				Logger.error("Exception = " + e.getMessage());
			}
		}

		return userId;
	}

	public static String updateUserSettings(Map<String, String[]> settings, String user)
	{
		String message = "Internal error";
		if (settings == null || settings.size() == 0)
		{
			return "Empty post body";
		}

		String defaultView = "";
		if (settings.containsKey("detail_default_view"))
    	{
      		String[] defaultViewArray = settings.get("detail_default_view");
      		if (defaultViewArray != null && defaultViewArray.length > 0)
      		{
        		defaultView = defaultViewArray[0];
      		}
		}
		String defaultWatch = "";
		if (settings.containsKey("default_watch"))
    	{
      		String[] defaultWatchArray = settings.get("default_watch");
      		if (defaultWatchArray != null && defaultWatchArray.length > 0)
      		{
        		defaultWatch = defaultWatchArray[0];
      		}
		}
		Integer userId = 0;
		if (StringUtils.isNotBlank(user))
		{
			try
			{
				userId = (Integer)getJdbcTemplate().queryForObject(
						GET_USER_ID,
						Integer.class,
						user);
			}
			catch(EmptyResultDataAccessException e)
			{
				Logger.error("UserDAO updateUserSettings get user id failed, username = " + user);
				Logger.error("Exception = " + e.getMessage());
			}
		}
		if (userId != null && userId > 0)
		{
			if (StringUtils.isBlank(defaultView))
			{
				defaultView = DEFAULT_DETAIL_VIEW;
			}

			if (StringUtils.isBlank(defaultWatch))
			{
				defaultWatch = DEFAULT_WATCH;
			}

			int row = getJdbcTemplate().update(UPDATE_USER_SETTINGS,
					userId,
					defaultView,
					defaultWatch,
					defaultView,
					defaultWatch);
			if (row > 0)
			{
				message = "";
			}
		}
		else
		{
			message = "User not found";
		}
		return message;
	}

	public static List<CompanyUser> getAllCompanyUsers()
	{
		List<CompanyUser> users = new ArrayList<CompanyUser>();
		List<Map<String, Object>> rows = null;
		rows = getJdbcTemplate().queryForList(
				GET_ALL_COMPANY_USERS);
		if (rows != null)
		{
			for (Map row : rows) {
				String userName = (String)row.get(UserRowMapper.USER_ID_COLUMN);
				String displayName = (String)row.get(UserRowMapper.USER_FULL_NAME_COLUMN);
				if (StringUtils.isNotBlank(userName))
				{
					CompanyUser user = new CompanyUser();
					user.userName = userName;
					user.displayName = displayName;
					users.add(user);
				}
			}
		}
		return users;
	}

	public static List<Group> getAllGroups()
	{
		List<Group> groups = new ArrayList<Group>();
		List<Map<String, Object>> rows = null;
		rows = getJdbcTemplate().queryForList(
				GET_ALL_GROUPS);
		if (rows != null)
		{
			for (Map row : rows) {
				String name = (String)row.get(UserRowMapper.USER_FULL_NAME_COLUMN);
				if (StringUtils.isNotBlank(name))
				{
					Group group = new Group();
					group.name = name;
					groups.add(group);
				}
			}
		}
		return groups;
	}

	public static List<UserEntity> getAllUserEntities()
	{
		List<UserEntity> userEntities = new ArrayList<UserEntity>();
		List<Map<String, Object>> rows = null;
		rows = getJdbcTemplate().queryForList(
				GET_ALL_COMPANY_USERS_AND_GROUPS);
		if (rows != null)
		{
			for (Map row : rows) {
				String label = (String)row.get(UserRowMapper.USER_ID_COLUMN);
				String displayName = (String)row.get(UserRowMapper.USER_FULL_NAME_COLUMN);
				String category = (String)row.get(UserRowMapper.CATEGORY_COLUMN);
				if (StringUtils.isNotBlank(label))
				{
					UserEntity user = new UserEntity();
					user.label = label;
					user.displayName = displayName;
					user.category = category;
					userEntities.add(user);
				}
			}
		}
		return userEntities;
	}

	public static void insertLoginHistory(String username, String loginType, String status, String message) {
		if (username != null && loginType != null && status != null) {
			getJdbcTemplate().update(INSERT_USER_LOGIN_HISTORY, username, loginType, status, message);
		}
	}
}
