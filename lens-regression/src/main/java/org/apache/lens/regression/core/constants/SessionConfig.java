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

package org.apache.lens.regression.core.constants;

public class SessionConfig {

  private SessionConfig() {

  }

  public static final String SERVER_PERSISTENCE = "lens.query.enable.persistent.resultset";
  public static final String DRIVER_PERSISTENCE = "lens.query.enable.persistent.resultset.indriver";
  public static final String DATA_PARTIAL = "lens.cube.query.fail.if.data.partial";
  public static final String QUERY_RESULT_DIR = "lens.query.result.parent.dir";
  public static final String MAPRED_JOB_QUEUE = "mapred.job.queue.name";
  public static final String RESULT_SERDE = "lens.query.result.output.serde";
  public static final String ENABLE_MAIL_NOTIFY = "lens.query.enable.mail.notify";

}

