/*
 * Copyright 2016 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.spark.deploy.history;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.Test;

public class SparkDataCollectionTest {

    private static final String event_log_dir = "spark_event_logs/";

    @Test
    public void testCollectJobProgressData() throws IOException {
        SparkDataCollection dataCollection = new SparkDataCollection();

        InputStream in = new BufferedInputStream(
                SparkDataCollectionTest.class.getClassLoader().getResourceAsStream(event_log_dir + "event_log_1"));
        dataCollection.replayEventLogs(in, in.toString());
        in.close();
    }

}
