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

package io.openmessaging.storage.dledger.store;

import io.openmessaging.storage.dledger.MemberState;
import io.openmessaging.storage.dledger.entry.DLedgerEntry;

/**
 * 存储抽象类
 */
public abstract class DLedgerStore {

    public MemberState getMemberState() {
        return null;
    }

    /**
     * 向主节点追加日志(数据)
     * @param entry 日志条目
     * @return 日志条目
     */
    public abstract DLedgerEntry appendAsLeader(DLedgerEntry entry);

    /**
     * 从节点追加日志
     * @param entry 日志条目
     * @param leaderTerm 投票轮次
     * @param leaderId Leader ID
     * @return 日志条目
     */
    public abstract DLedgerEntry appendAsFollower(DLedgerEntry entry, long leaderTerm, String leaderId);

    /**
     * 根据日志索引查找日志
     * @param index 索引
     * @return 日志条目
     */
    public abstract DLedgerEntry get(Long index);

    /**
     * 获取已提交的下标
     * @return 索引
     */
    public abstract long getCommittedIndex();

    /**
     * 更新已提交日志索引
     * @param term 选举轮次
     * @param committedIndex 已提交日志索引
     */
    public void updateCommittedIndex(long term, long committedIndex) {

    }

    /**
     * 获取Leader当前最大的投票轮次
     * @return 投票轮次
     */
    public abstract long getLedgerEndTerm();

    /**
     * 获取Leader下一条日志写入的索引
     * @return 索引
     */
    public abstract long getLedgerEndIndex();

    /**
     * 获取Leader第一条消息的下标
     * @return 索引
     */
    public abstract long getLedgerBeginIndex();

    /**
     * 更新写入索引和选举轮次
     */
    protected void updateLedgerEndIndexAndTerm() {
        if (getMemberState() != null) {
            getMemberState().updateLedgerIndexAndTerm(getLedgerEndIndex(), getLedgerEndTerm());
        }
    }

    /**
     * 刷写
     */
    public void flush() {

    }

    /**
     * 删除日志
     * @param entry 日志条目
     * @param leaderTerm 选举轮次
     * @param leaderId Leader ID
     * @return long
     */
    public long truncate(DLedgerEntry entry, long leaderTerm, String leaderId) {
        return -1;
    }

    /**
     * 启动
     */
    public void startup() {

    }

    /**
     * 停止
     */
    public void shutdown() {

    }
}
