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
import org.junit.*;

import play.libs.ws.WS;

import static play.test.Helpers.*;
import static org.fest.assertions.api.Assertions.*;

@Ignore
public class IntegrationTest {

    @Test
    public void test() {

        running(testServer(3333), new Runnable() {
            public void run() {
                assertThat(WS.url("http://localhost:3333").get().get(1000*60).getStatus()).isEqualTo(OK);
            }
        });
    }

}
