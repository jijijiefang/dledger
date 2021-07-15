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

package io.openmessaging.storage.dledger.protocol;

import static io.openmessaging.storage.dledger.protocol.VoteResponse.RESULT.UNKNOWN;

public class VoteResponse extends RequestOrResponse {

    public RESULT voteResult = UNKNOWN;

    public VoteResponse() {

    }

    public VoteResponse(VoteRequest request) {
        copyBaseInfo(request);
    }

    public RESULT getVoteResult() {
        return voteResult;
    }

    public void setVoteResult(RESULT voteResult) {
        this.voteResult = voteResult;
    }

    public VoteResponse voteResult(RESULT voteResult) {
        this.voteResult = voteResult;
        return this;
    }

    public VoteResponse term(long term) {
        this.term = term;
        return this;
    }

    public enum RESULT {
        /**
         * 未知
         */
        UNKNOWN,
        /**
         * 赞成票
         */
        ACCEPT,
        /**
         *
         */
        REJECT_UNKNOWN_LEADER,
        REJECT_UNEXPECTED_LEADER,
        /**
         * 拒绝票，如果自己维护的term 小于远端维护的term，更新自己维护的投票轮次
         */
        REJECT_EXPIRED_VOTE_TERM,
        /**
         * 拒绝票，原因是已经投了其他节点的票
         */
        REJECT_ALREADY_VOTED,
        /**
         * 拒绝票，原因是因为集群中已经存在Leader了
         */
        REJECT_ALREADY_HAS_LEADER,
        /**
         * 拒绝票，对端的投票轮次小于自己的team，则认为对端还未准备好投票，对端使用自
         * 己的投票轮次，是自己进入到Candidate 状态。
         */
        REJECT_TERM_NOT_READY,
        /**
         * 拒绝票，如果自己维护的term 小于远端维护的ledgerEndTerm，则返回该结果
         */
        REJECT_TERM_SMALL_THAN_LEDGER,
        /**
         * 拒绝票，如果自己维护的ledgerTerm 小于对端维护的ledgerTerm，则返回该结果
         */
        REJECT_EXPIRED_LEDGER_TERM,
        /**
         * 拒绝票，如果对端的ledgerTeam 与自己维护的ledgerTeam 相等，但是自己维护的
         * dedgerEndIndex 小于对端维护的值，返回该值，增加biggerLedgerNum 计数器的值
         */
        REJECT_SMALL_LEDGER_END_INDEX,
        REJECT_TAKING_LEADERSHIP;
    }

    public enum ParseResult {
        WAIT_TO_REVOTE,
        REVOTE_IMMEDIATELY,
        PASSED,
        WAIT_TO_VOTE_NEXT;
    }
}
