package com.wso2cre.asyncdemo;

import org.apache.axis2.Constants;
import org.apache.synapse.MessageContext;
import org.apache.synapse.commons.json.JsonUtil;
import org.apache.synapse.core.axis2.Axis2MessageContext;
import org.apache.synapse.core.axis2.Axis2Sender;
import org.apache.synapse.mediators.AbstractMediator;
import org.apache.synapse.message.store.MessageStore;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

/**
 * Retrieves the parked MessageContext for the correlation id carried by the current (callback)
 * message from the message store named by MESSAGE_STORE_NAME, attaches the callback's JSON
 * payload to it, and completes the response on the ORIGINAL client connection retained inside
 * that parked context.
 *
 * Uses MessageStore.remove(String) directly - a standard interface method - so this mediator
 * works against ANY MessageStore implementation. With ExpiringConcurrentMapMessageStore this is
 * an O(1) lookup keyed by CORRELATION_ID (not MessageContext.getMessageID() - see that class's
 * javadoc for why); with the stock InMemoryStore it would still work, just via that store's own
 * (slower, id-mismatched) semantics - which is exactly the throughput problem this custom store
 * exists to fix.
 *
 * Relies on the deploying sequence parking the request (`<store>`) BEFORE calling the backend, not
 * after - see ExpiringConcurrentMapMessageStore's class javadoc for why that ordering rules out a
 * callback ever arriving before its request has parked.
 *
 * Expects these message-context properties to already be set (via <property> mediators) before
 * this mediator runs:
 *   - MESSAGE_STORE_NAME : name of the message store the request was parked in (required)
 *   - CORRELATION_ID     : the id used to look up the parked request in that store (required)
 *
 * Sets on the CURRENT (callback) message context, for the callback sequence to branch on:
 *   - COMPLETE_RESULT : "OK" | "NOT_FOUND" | "NO_CORRELATION_ID" | "STORE_NOT_FOUND" | "ERROR"
 */
public class CompleteRequestMediator extends AbstractMediator {

    public boolean mediate(MessageContext synCtx) {

        Object storeNameObj = synCtx.getProperty("MESSAGE_STORE_NAME");
        Object correlationIdObj = synCtx.getProperty("CORRELATION_ID");

        String storeName = storeNameObj != null ? storeNameObj.toString() : null;
        String correlationId = correlationIdObj != null ? correlationIdObj.toString() : null;

        if (storeName == null || storeName.isEmpty()) {
            log.error("CompleteRequestMediator: MESSAGE_STORE_NAME property not set.");
            synCtx.setProperty("COMPLETE_RESULT", "STORE_NOT_FOUND");
            return true;
        }

        if (correlationId == null || correlationId.isEmpty()) {
            log.warn("CompleteRequestMediator: CORRELATION_ID property not set - cannot complete a parked request.");
            synCtx.setProperty("COMPLETE_RESULT", "NO_CORRELATION_ID");
            return true;
        }

        MessageStore store = synCtx.getConfiguration().getMessageStore(storeName);
        if (store == null) {
            log.error("CompleteRequestMediator: message store [" + storeName + "] not found.");
            synCtx.setProperty("COMPLETE_RESULT", "STORE_NOT_FOUND");
            return true;
        }

        MessageContext parkedCtx = store.remove(correlationId);
        if (parkedCtx == null) {
            log.warn("CompleteRequestMediator: no parked request found for correlation id [" + correlationId
                    + "] in store [" + storeName + "] - already completed, expired, or unknown id.");
            synCtx.setProperty("COMPLETE_RESULT", "NOT_FOUND");
            return true;
        }

        try {
            org.apache.axis2.context.MessageContext callbackAxis2Ctx =
                    ((Axis2MessageContext) synCtx).getAxis2MessageContext();
            String resultJson = JsonUtil.jsonPayloadToString(callbackAxis2Ctx);
            if (resultJson == null) {
                resultJson = "{}";
            }

            org.apache.axis2.context.MessageContext parkedAxis2Ctx =
                    ((Axis2MessageContext) parkedCtx).getAxis2MessageContext();

            InputStream in = toInputStream(resultJson);
            JsonUtil.newJsonPayload(parkedAxis2Ctx, in, true, true);
            parkedAxis2Ctx.setProperty(Constants.Configuration.MESSAGE_TYPE, "application/json");
            parkedAxis2Ctx.setProperty(Constants.Configuration.CONTENT_TYPE, "application/json");
            parkedAxis2Ctx.setProperty("HTTP_SC", "200");
            // Clears the 202-forcing flag a 202 backend ack leaves on this context (see
            // PassThroughTransportUtils - httpStatus is forced to SC_ACCEPTED whenever this is true).
            parkedAxis2Ctx.setProperty("SC_ACCEPTED", Boolean.FALSE);

            parkedCtx.setTo(null);
            parkedCtx.setResponse(true);
            parkedAxis2Ctx.getOperationContext().setProperty(Constants.RESPONSE_WRITTEN, "SKIP");
            Axis2Sender.sendBack(parkedCtx);

            synCtx.setProperty("COMPLETE_RESULT", "OK");
            if (log.isDebugEnabled()) {
                log.debug("CompleteRequestMediator: completed parked request for correlation id [" + correlationId + "]");
            }
        } catch (Exception e) {
            log.error("CompleteRequestMediator: failed to complete parked request for correlation id ["
                    + correlationId + "]", e);
            synCtx.setProperty("COMPLETE_RESULT", "ERROR");
        }

        return true;
    }

    private InputStream toInputStream(String s) throws UnsupportedEncodingException {
        return new ByteArrayInputStream(s.getBytes("UTF-8"));
    }

    @Override
    public boolean isContentAware() {
        return false;
    }
}
