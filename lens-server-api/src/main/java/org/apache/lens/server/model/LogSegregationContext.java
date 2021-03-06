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
package org.apache.lens.server.model;

public interface LogSegregationContext {

  /**
   * Sets an id to be used by current thread in every log line for log segregation
   *
   * @param id the id to be added to every log line of current thread
   */
  void setLogSegregationId(final String id);

  /**
   *
   * @return the id being used by the current thread for log segregation
   */
  String getLogSegragationId();

  /**
   * Sets query id to be used by current thread for log segregation for identifying current query.
   * The same id is set as log segregation in every log line as well.
   *
   * @param id the query id
   */
  void setLogSegragationAndQueryId(final String id);

  /**
   * Sets query id to be used by current thread for log segregation for identifying current query.
   *
   * @param id the query id
   */
  void setQueryId(final String id);

  /**
   * Get current query id in log segregation
   *
   * @return the query id being used by the current thread for log segregation
   */
  String getQueryId();

}
