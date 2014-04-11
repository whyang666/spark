/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.execution

import org.apache.spark.sql.hive.TestHive._

/**
 * A set of test cases expressed in Panthera support that are not covered by the tests supported in hive.
 */
class PantheraQuerySuite extends HiveComparisonTest {
  createQueryTest("irregular order name 1",
    """SELECT * FROM src order by src.key""")

  createQueryTest("Simple uncorrelated",
    """SELECT key FROM src a WHERE EXISTS (SELECT * FROM src b WHERE b.key = 68)""")

  createQueryTest("Simple correlated",
    """SELECT key FROM src a WHERE exists (select * from src b where a.key = b.key)""")

  createQueryTest("tt correlated",
    """SELECT a.key FROM src a LEFT OUTER JOIN src b on a.key = b.key""")

}
