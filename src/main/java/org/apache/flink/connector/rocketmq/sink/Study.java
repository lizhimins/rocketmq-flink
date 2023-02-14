/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.rocketmq.sink;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class Study {

    public static void main(String[] args) throws BrokenBarrierException, InterruptedException {

        CyclicBarrier cyclicBarrier = new CyclicBarrier(3);

        new Thread(() -> {
            try {
                Thread.sleep(10_000);
                System.out.println("t1 在准备 ");
                cyclicBarrier.await();   // 等另外一个一个线程准备好 然后开始做事情
                System.out.println("t1 准备好了  ");
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }

        }).start();

        new Thread(() -> {
            try {
                Thread.sleep(5_000);
                System.out.println("t2 在准备  ");
                cyclicBarrier.await(); // 等另外一个一个线程准备好 然后开始做事情
                System.out.println("t2 准备好了  ");
            } catch (InterruptedException | BrokenBarrierException e) {
                e.printStackTrace();
            }
        }).start();


        System.out.println(" 裁判 在准备 ");
        cyclicBarrier.await();
        System.out.println(" 裁判 准备好了 ");


        while (true) {
            if (cyclicBarrier.getNumberWaiting() == 0) {
                System.out.println(" 时间到: 开始比赛");
                return;
            }
        }
    }
}
