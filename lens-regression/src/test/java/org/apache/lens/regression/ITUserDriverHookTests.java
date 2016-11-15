/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.lens.regression;

import java.lang.reflect.Method;
import java.util.HashMap;

import javax.ws.rs.client.WebTarget;

import org.apache.lens.api.query.*;
import org.apache.lens.api.result.LensAPIResult;
import org.apache.lens.cube.parse.CubeQueryConfUtil;
import org.apache.lens.regression.core.constants.DriverConfig;
import org.apache.lens.regression.core.helpers.ServiceManagerHelper;
import org.apache.lens.regression.core.testHelper.BaseTestClass;
import org.apache.lens.regression.util.Util;
import org.apache.lens.server.api.LensConfConstants;
import org.apache.lens.server.api.util.LensUtil;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.*;


public class ITUserDriverHookTests extends BaseTestClass {

  WebTarget servLens;
  private String sessionHandleString;
  private static Logger logger = Logger.getLogger(ITUserDriverHookTests.class);

  String jdbcDriver1 = "jdbc/prod", jdbcDriver2 = "jdbc/clarity", hiveDriver1 = "hive/prod";
  String jdbcConf1 = lens.getServerDir() + "/conf/drivers/" + jdbcDriver1 + "/jdbcdriver-site.xml";
  String jdbcConf2 = lens.getServerDir() + "/conf/drivers/" + jdbcDriver2 + "/jdbcdriver-site.xml";
  String hiveconf1 = lens.getServerDir() + "/conf/drivers/" + hiveDriver1 + "/hivedriver-site.xml";
  String hookSuffix = LensConfConstants.DRIVER_HOOK_CLASSES_SFX;

  @BeforeClass(alwaysRun = true)
  public void initialize() throws Exception {
    servLens = ServiceManagerHelper.init();
  }

  @BeforeMethod(alwaysRun = true)
  public void setUp(Method method) throws Exception {
    logger.info("Test Name: " + method.getName());
    sessionHandleString = sHelper.openSession(lens.getCurrentDB());
  }

  @AfterMethod(alwaysRun = true)
  public void closeSession() throws Exception {
    logger.info("Closing Session");
    sHelper.closeSession();
  }

  @Test(enabled = true)
  public void testUserBasedQueryHook() throws Exception {

    String query = "cube select total_burn from rrcube where time_range_in(event_time, '2016-11-01', '2016-11-02')";
    try{
      HashMap<String, String> map1 = LensUtil.getHashMap(hookSuffix,
          "org.apache.lens.server.api.driver.hooks.UserBasedQueryHook", "disallowed.users", "clarity");

      HashMap<String, String> map2 = LensUtil.getHashMap("lens.driver.jdbc.query.hook.classes",
          "org.apache.lens.server.api.driver.hooks.UserBasedQueryHook", "allowed.users", "clarity");

      Util.changeConfig(map1, jdbcConf1, "prod-jdbcdriver-site.xml");
      Util.changeConfig(map2, jdbcConf2, "clarity-jdbcdriver-site.xml");

      lens.restart();

      String session1 = sHelper.openSession("user1", "pwd1", lens.getCurrentDB());
      String session2 = sHelper.openSession("clarity", "clarity", lens.getCurrentDB());
      sHelper.setAndValidateParam(session1, CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
      sHelper.setAndValidateParam(session2, CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(query, null, session1).getData();
      QueryHandle q2 = (QueryHandle) qHelper.executeQuery(query, null, session2).getData();

      LensQuery lq1 = qHelper.getLensQuery(session1, q1);
      LensQuery lq2 = qHelper.getLensQuery(session2, q2);

      Assert.assertEquals(lq1.getSelectedDriverName(), jdbcDriver1);
      Assert.assertEquals(lq2.getSelectedDriverName(), jdbcDriver2);

      lq1 = qHelper.waitForCompletion(q1);
      lq2 = qHelper.waitForCompletion(q2);

      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);
      Assert.assertEquals(lq2.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    } finally{

      Util.changeConfig(jdbcConf1, "prod-jdbcdriver-site.xml");
      Util.changeConfig(jdbcConf2, "clarity-jdbcdriver-site.xml");
      lens.restart();
    }
  }



  @Test(enabled = true)
  public void testMultipleDisallowedUser() throws Exception {

    String query = "cube select total_burn from rrcube where time_range_in(event_time, '2016-11-01', '2016-11-02')";
    try{
      HashMap<String, String> map1 = LensUtil.getHashMap(hookSuffix,
          "org.apache.lens.server.api.driver.hooks.UserBasedQueryHook", "disallowed.users", "hook-user1,hook-user2");
      Util.changeConfig(map1, jdbcConf1);
      lens.restart();

      String[] users = {"hook-user1", "hook-user2", "user"};
      String[] expected = {hiveDriver1, hiveDriver1, jdbcDriver1};

      for(int i = 0; i < users.length; i++){
        String session = sHelper.openSession(users[i], "pwd", lens.getCurrentDB());
        sHelper.setAndValidateParam(session, CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
        QueryHandle q = (QueryHandle) qHelper.executeQuery(query, null, session).getData();
        LensQuery lq = qHelper.getLensQuery(session, q);
        Assert.assertEquals(lq.getSelectedDriverName(), expected[i]);
      }

    } finally {
      Util.changeConfig(jdbcConf1);
      lens.restart();
    }
  }

  @Test(enabled = true)
  public void notAnswerableByAnyDriver() throws Exception {

    String query = "cube select total_burn from rrcube where time_range_in(event_time, '2016-11-01', '2016-11-02')";
    try{
      HashMap<String, String> map1 = LensUtil.getHashMap(hookSuffix,
          "org.apache.lens.server.api.driver.hooks.UserBasedQueryHook", "disallowed.users", "hook-user");
      Util.changeConfig(map1, jdbcConf1);
      Util.changeConfig(map1, hiveconf1);

      lens.restart();

      String session1 = sHelper.openSession("user1", "pwd1", lens.getCurrentDB());
      String session2 = sHelper.openSession("hook-user", "pwd2", lens.getCurrentDB());
      sHelper.setAndValidateParam(session1, CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
      sHelper.setAndValidateParam(session2, CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");

      QueryHandle q1 = (QueryHandle) qHelper.executeQuery(query, null, session1).getData();
      LensQuery lq1 = qHelper.getLensQuery(session1, q1);
      Assert.assertEquals(lq1.getSelectedDriverName(), jdbcDriver1);

      LensAPIResult res = qHelper.estimateQuery(query, session2);
      Assert.assertTrue(res.isErrorResult());

      lq1 = qHelper.waitForCompletion(q1);
      Assert.assertEquals(lq1.getStatus().getStatus(), QueryStatus.Status.SUCCESSFUL);

    } finally {
      Util.changeConfig(jdbcConf1);
      Util.changeConfig(hiveconf1);
      lens.restart();
    }
  }

  @Test(enabled = true)
  public void allowDisallowMutlipltUsers() throws Exception {

    String query = "cube select total_burn from rrcube where time_range_in(event_time, '2016-11-01', '2016-11-02')";
    try{
      HashMap<String, String> map1 = LensUtil.getHashMap(hookSuffix,
          DriverConfig.USER_BASED_QUERY_HOOK, DriverConfig.DISALLOWED_USERS, "clarity,hook-user1,hook-user2");

      HashMap<String, String> map2 = LensUtil.getHashMap("lens.driver.jdbc.query.hook.classes",
          "org.apache.lens.server.api.driver.hooks.UserBasedQueryHook", DriverConfig.ALLOWED_USERS,
          "clarity,hook-user1,hook-user2");

      Util.changeConfig(map1, jdbcConf1, "prod-jdbcdriver-site.xml");
      Util.changeConfig(map2, jdbcConf2, "clarity-jdbcdriver-site.xml");

      lens.restart();

      String[] users = {"clarity", "hook-user2", "user"};
      String[] expected = {jdbcDriver2, jdbcDriver2, jdbcDriver1};

      for(int i = 0; i < users.length; i++){
        String session = sHelper.openSession(users[i], "pwd", lens.getCurrentDB());
        sHelper.setAndValidateParam(session, CubeQueryConfUtil.FAIL_QUERY_ON_PARTIAL_DATA, "false");
        QueryHandle q = (QueryHandle) qHelper.executeQuery(query, null, session).getData();
        LensQuery lq = qHelper.getLensQuery(session, q);
        Assert.assertEquals(lq.getSelectedDriverName(), expected[i]);
      }

    } finally{
      Util.changeConfig(jdbcConf1, "prod-jdbcdriver-site.xml");
      Util.changeConfig(jdbcConf2, "clarity-jdbcdriver-site.xml");
      lens.restart();
    }
  }

}
