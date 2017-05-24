/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.communication.tcp;

import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.text.*;
import java.util.*;

/**
 * TODO
 */
public class TestDebugLog {
    /** */
    private static final List<Object> msgs = Collections.synchronizedList(new ArrayList<>(100_000));

    /** */
    private static final SimpleDateFormat DEBUG_DATE_FMT = new SimpleDateFormat("HH:mm:ss,SSS");

    static class Message {
        String thread = Thread.currentThread().getName();

        String msg;

        long ts = U.currentTimeMillis();

        public Message(String msg) {
            this.msg = msg;
        }

        public String toString() {
            return "Msg [msg=" + msg + ", thread=" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
        }
    }

    static class EntryMessage extends Message {
        Object key;
        Object val;

        public EntryMessage(Object key, Object val, String msg) {
            super(msg);

            this.key = key;
            this.val = val;
        }

        public String toString() {
            return "EntryMsg [key=" + key + ", val=" + val + ", msg=" + msg + ", thread" + thread + ", time=" + DEBUG_DATE_FMT.format(new Date(ts)) + ']';
        }
    }

    static final boolean out = false;

    public static void addMessage(String msg) {
        msgs.add(new Message(msg));

        if (out)
            System.out.println(msg);
    }

    public static void addEntryMessage(Object key, Object val, String msg) {
        EntryMessage msg0 = new EntryMessage(key, val, msg);

        msgs.add(msg0);

        if (out)
            System.out.println(msg0.toString());
    }

    public static void printMessages(boolean file) {
        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            msgs.clear();
        }

        if (file) {
            try {
                FileOutputStream out = new FileOutputStream("test_debug.log");

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0)
                    w.println(msg.toString());

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0)
                System.out.println(msg);
        }
    }

    public static void printKeyMessages(boolean file, Object key) {
        List<Object> msgs0;

        synchronized (msgs) {
            msgs0 = new ArrayList<>(msgs);

            msgs.clear();
        }

        if (file) {
            try {
                FileOutputStream out = new FileOutputStream("test_debug.log");

                PrintWriter w = new PrintWriter(out);

                for (Object msg : msgs0) {
                    if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                        continue;

                    w.println(msg.toString());
                }

                w.close();

                out.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            for (Object msg : msgs0) {
                if (msg instanceof EntryMessage && !((EntryMessage)msg).key.equals(key))
                    continue;

                System.out.println(msg);
            }
        }
    }

    public static void clear() {
        msgs.clear();
    }

    public static void main(String[] args) {
    }
}
