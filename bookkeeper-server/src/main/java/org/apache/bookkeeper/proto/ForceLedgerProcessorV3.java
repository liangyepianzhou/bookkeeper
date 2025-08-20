/*
 *
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
 *
 */
package org.apache.bookkeeper.proto;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.common.util.MathUtils;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ForceLedgerRequest;
import org.apache.bookkeeper.proto.BookkeeperProtocol.ForceLedgerResponse;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Request;
import org.apache.bookkeeper.proto.BookkeeperProtocol.Response;
import org.apache.bookkeeper.proto.BookkeeperProtocol.StatusCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class ForceLedgerProcessorV3 extends PacketProcessorBaseV3 implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(ForceLedgerProcessorV3.class);

    public ForceLedgerProcessorV3(Request request, BookieRequestHandler requestHandler,
                             BookieRequestProcessor requestProcessor) {
        super(request, requestHandler, requestProcessor);
    }

    // 返回 ForceLedgerResponse，如果一切正常则返回 null（结果将在回调异步返回）
    private ForceLedgerResponse getForceLedgerResponse() {
        // 记录操作开始时间，方便统计耗时
        final long startTimeNanos = MathUtils.nowInNano();

        // 从请求中提取 ForceLedgerRequest
        ForceLedgerRequest forceLedgerRequest = request.getForceLedgerRequest();
        long ledgerId = forceLedgerRequest.getLedgerId(); // 获取 ledger 的唯一ID

        // 构造 ForceLedgerResponse（builder）
        final ForceLedgerResponse.Builder forceLedgerResponse = ForceLedgerResponse.newBuilder().setLedgerId(ledgerId);

        // 版本兼容性检查
        if (!isVersionCompatible()) {
            forceLedgerResponse.setStatus(StatusCode.EBADVERSION); // 设置版本错误状态码
            return forceLedgerResponse.build(); // 如果版本不兼容，直接返回错误响应
        }

        // 异步写回调函数
        BookkeeperInternalCallbacks.WriteCallback wcb =
                (int rc, long ledgerId1, long entryId, BookieId addr, Object ctx) -> {

                    // 校验 entryId 必须是 METAENTRY_ID_FORCE_LEDGER，防止调用参数混乱
                    checkArgument(entryId == BookieImpl.METAENTRY_ID_FORCE_LEDGER,
                            "entryId must be METAENTRY_ID_FORCE_LEDGER but was {}", entryId);

                    // 校验 ledgerId 是否一致
                    checkArgument(ledgerId1 == ledgerId,
                            "ledgerId must be {} but was {}", ledgerId, ledgerId1);

                    // 根据操作结果（返回码）记录成功或失败的统计信息
                    if (BookieProtocol.EOK == rc) {
                        requestProcessor.getRequestStats().getForceLedgerStats()
                                .registerSuccessfulEvent(MathUtils.elapsedNanos(startTimeNanos),
                                        TimeUnit.NANOSECONDS);
                    } else {
                        requestProcessor.getRequestStats().getForceLedgerStats()
                                .registerFailedEvent(MathUtils.elapsedNanos(startTimeNanos),
                                        TimeUnit.NANOSECONDS);
                    }

                    // 状态码转换
                    StatusCode status;
                    switch (rc) {
                        case BookieProtocol.EOK: // 成功
                            status = StatusCode.EOK;
                            break;
                        case BookieProtocol.EIO: // IO异常
                            status = StatusCode.EIO;
                            break;
                        default: // 其他未知异常
                            status = StatusCode.EUA;
                            break;
                    }
                    // 应答设置状态码
                    forceLedgerResponse.setStatus(status);
                    // 构造最终 Response，带上 header 和响应体
                    Response.Builder response = Response.newBuilder()
                            .setHeader(getHeader())
                            .setStatus(forceLedgerResponse.getStatus())
                            .setForceLedgerResponse(forceLedgerResponse);
                    Response resp = response.build();
                    // 发送结果给客户端，并统计请求信息
                    sendResponse(status, resp, requestProcessor.getRequestStats().getForceLedgerRequestStats());
                };

        StatusCode status = null;
        try {
            // 调用底层 Bookie 的 forceLedger 方法，异步执行刷盘
            requestProcessor.getBookie().forceLedger(ledgerId, wcb, requestHandler);
            status = StatusCode.EOK; // 操作成功，状态码设为 EOK
        } catch (Throwable t) {
            // 捕获异常并记录日志
            logger.error("Unexpected exception while forcing ledger {} : ", ledgerId, t);
            // 输入参数错误等异常处理
            status = StatusCode.EBADREQ; // 设置错误请求状态码
        }

        // 如果不是 EOK，则说明有异常，需要返回 ForceLedgerResponse
        // 如果正常（EOK），整个过程异步，等回调中由 wcb 返回结果，不需要立即应答
        if (!status.equals(StatusCode.EOK)) {
            forceLedgerResponse.setStatus(status);
            return forceLedgerResponse.build();
        }
        return null;
    }


    @Override
    public void run() {
        ForceLedgerResponse forceLedgerResponse = getForceLedgerResponse();
        if (null != forceLedgerResponse) {
            Response.Builder response = Response.newBuilder()
                    .setHeader(getHeader())
                    .setStatus(forceLedgerResponse.getStatus())
                    .setForceLedgerResponse(forceLedgerResponse);
            Response resp = response.build();
            sendResponse(
                forceLedgerResponse.getStatus(),
                resp,
                requestProcessor.getRequestStats().getForceLedgerRequestStats());
        }
    }

    /**
     * this toString method filters out body and masterKey from the output.
     * masterKey contains the password of the ledger and body is customer data,
     * so it is not appropriate to have these in logs or system output.
     */
    @Override
    public String toString() {
        return RequestUtils.toSafeString(request);
    }
}


