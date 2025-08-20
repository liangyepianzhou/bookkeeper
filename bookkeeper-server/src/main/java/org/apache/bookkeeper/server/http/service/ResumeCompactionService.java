/*
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
package org.apache.bookkeeper.server.http.service;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.HashMap;
import java.util.Map;
import org.apache.bookkeeper.common.util.JsonUtil;
import org.apache.bookkeeper.http.HttpServer;
import org.apache.bookkeeper.http.service.HttpEndpointService;
import org.apache.bookkeeper.http.service.HttpServiceRequest;
import org.apache.bookkeeper.http.service.HttpServiceResponse;
import org.apache.bookkeeper.proto.BookieServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResumeCompactionService implements HttpEndpointService {

    static final Logger LOG = LoggerFactory.getLogger(ResumeCompactionService.class);

    protected BookieServer bookieServer;

    public ResumeCompactionService(BookieServer bookieServer) {
        checkNotNull(bookieServer);
        this.bookieServer = bookieServer;
    }

    /**
     * 处理 HTTP 请求，根据请求开启 BookieServer 的 MajorGC 和 MinorGC 日志压缩功能。
     *
     * 接受 HTTP PUT 请求，body 应包含 resumeMajor、resumeMinor 两个参数，用于决定是否恢复对应的 GC。
     */
    @Override
    public HttpServiceResponse handle(HttpServiceRequest request) throws Exception {
        HttpServiceResponse response = new HttpServiceResponse();

        // 只处理 PUT 方法
        if (HttpServer.Method.PUT == request.getMethod()) {
            String requestBody = request.getBody();
            // 请求体为空，返回 400 错误
            if (null == requestBody) {
                return new HttpServiceResponse("Empty request body", HttpServer.StatusCode.BAD_REQUEST);
            } else {
                // 反序列化 JSON 格式的请求体
                @SuppressWarnings("unchecked")
                Map<String, Object> configMap = JsonUtil.fromJson(requestBody, HashMap.class);

                // 获取参数 resumeMajor、resumeMinor，决定是否恢复 Major/Minor GC
                Boolean resumeMajor = (Boolean) configMap.get("resumeMajor");
                Boolean resumeMinor = (Boolean) configMap.get("resumeMinor");

                // 两个参数均未提供，也返回 400 错误
                if (resumeMajor == null && resumeMinor == null) {
                    return new HttpServiceResponse("No resumeMajor or resumeMinor params found",
                            HttpServer.StatusCode.BAD_REQUEST);
                }

                String output = "";
                // 根据参数决定是否恢复 Major GC
                if (resumeMajor != null && resumeMajor) {
                    output = "Resume majorGC on BookieServer: " + bookieServer.toString();
                    bookieServer.getBookie().getLedgerStorage().resumeMajorGC();
                }
                // 根据参数决定是否恢复 Minor GC
                if (resumeMinor != null && resumeMinor) {
                    output += ", Resume minorGC on BookieServer: " + bookieServer.toString();
                    bookieServer.getBookie().getLedgerStorage().resumeMinorGC();
                }

                // 返回操作结果（转换为 JSON 字符串格式）
                String jsonResponse = JsonUtil.toJson(output);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("output body:" + jsonResponse);
                }
                response.setBody(jsonResponse);
                response.setCode(HttpServer.StatusCode.OK);
                return response;
            }
        } else {
            // 非 PUT 请求不处理GC，返回 404 及说明
            response.setCode(HttpServer.StatusCode.NOT_FOUND);
            response.setBody("Not found method. Should be PUT to resume major or minor compaction, Or GET to get "
                    + "compaction state.");
            return response;
        }
    }
}