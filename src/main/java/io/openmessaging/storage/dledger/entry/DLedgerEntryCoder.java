/*
 * Copyright 2017-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.openmessaging.storage.dledger.entry;

import java.nio.ByteBuffer;

public class DLedgerEntryCoder {

    /**
     * 编码
     * @param entry 日志条目
     * @param byteBuffer 缓冲区
     */
    public static void encode(DLedgerEntry entry, ByteBuffer byteBuffer) {
        byteBuffer.clear();
        int size = entry.computSizeInBytes();
        //always put magic on the first position
        //魔数，4 字节
        byteBuffer.putInt(entry.getMagic());
        //条目总长度，包含Header(协议头) + 消息体，占4 字节
        byteBuffer.putInt(size);
        //当前条目的index，占8 字节
        byteBuffer.putLong(entry.getIndex());
        //当前条目所属的投票轮次，占8 字节
        byteBuffer.putLong(entry.getTerm());
        //该条目的物理偏移量，类似于commitlog 文件的物理偏移量，占8 字节
        byteBuffer.putLong(entry.getPos());
        //保留字段，当前版本未使用，占4 字节
        byteBuffer.putInt(entry.getChannel());
        //当前版本未使用，占4 字节
        byteBuffer.putInt(entry.getChainCrc());
        //body 的CRC 校验和，用来区分数据是否损坏，占4 字节。
        byteBuffer.putInt(entry.getBodyCrc());
        //用来存储body 的长度，占4 个字节。
        byteBuffer.putInt(entry.getBody().length);
        //具体消息的内容。
        byteBuffer.put(entry.getBody());
        byteBuffer.flip();
    }

    /**
     * 日志索引编码
     * @param pos 日志条目在文件的偏移量
     * @param size 条目大小
     * @param magic 魔数
     * @param index 索引
     * @param term 投票轮次
     * @param byteBuffer 缓冲区
     */
    public static void encodeIndex(long pos, int size, int magic, long index, long term, ByteBuffer byteBuffer) {
        byteBuffer.clear();
        //魔数，4 字节
        byteBuffer.putInt(magic);
        //日志条目在文件的偏移量，8字节
        byteBuffer.putLong(pos);
        //条目大小,4字节
        byteBuffer.putInt(size);
        //日志条目索引，8字节
        byteBuffer.putLong(index);
        //投票轮次，8字节
        byteBuffer.putLong(term);
        byteBuffer.flip();
    }

    public static DLedgerEntry decode(ByteBuffer byteBuffer) {
        return decode(byteBuffer, true);
    }

    public static DLedgerEntry decode(ByteBuffer byteBuffer, boolean readBody) {
        DLedgerEntry entry = new DLedgerEntry();
        entry.setMagic(byteBuffer.getInt());
        entry.setSize(byteBuffer.getInt());
        entry.setIndex(byteBuffer.getLong());
        entry.setTerm(byteBuffer.getLong());
        entry.setPos(byteBuffer.getLong());
        entry.setChannel(byteBuffer.getInt());
        entry.setChainCrc(byteBuffer.getInt());
        entry.setBodyCrc(byteBuffer.getInt());
        int bodySize = byteBuffer.getInt();
        if (readBody && bodySize < entry.getSize()) {
            byte[] body = new byte[bodySize];
            byteBuffer.get(body);
            entry.setBody(body);
        }
        return entry;
    }

    public static void setPos(ByteBuffer byteBuffer, long pos) {
        byteBuffer.mark();
        byteBuffer.position(byteBuffer.position() + DLedgerEntry.POS_OFFSET);
        byteBuffer.putLong(pos);
        byteBuffer.reset();
    }

    public static long getPos(ByteBuffer byteBuffer) {
        long pos;
        byteBuffer.mark();
        byteBuffer.position(byteBuffer.position() + DLedgerEntry.POS_OFFSET);
        pos = byteBuffer.getLong();
        byteBuffer.reset();
        return pos;
    }

    /**
     * 日志条目索引缓冲区
     * @param byteBuffer 缓冲区
     * @param index 索引
     * @param term 投票轮次
     * @param magic 魔数
     */
    public static void setIndexTerm(ByteBuffer byteBuffer, long index, long term, int magic) {
        byteBuffer.mark();
        byteBuffer.putInt(magic);
        byteBuffer.position(byteBuffer.position() + 4);
        byteBuffer.putLong(index);
        byteBuffer.putLong(term);
        byteBuffer.reset();
    }

}
