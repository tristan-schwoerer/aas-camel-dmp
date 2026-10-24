/*******************************************************************************
 * Copyright (C) 2023 the Eclipse BaSyx Authors
 * 
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 * 
 * SPDX-License-Identifier: MIT
 ******************************************************************************/

package dk.aau.smartproductionlab.dmp.aas;

import java.io.IOException;
import java.time.Duration;

import org.apache.camel.Exchange;
import org.apache.camel.support.PollingConsumerSupport;

import dk.aau.smartproductionlab.dmp.aas.http.AasHttp;
import dk.aau.smartproductionlab.dmp.core.util.JsonCodec;

/** One read of the endpoint's element per receive, on the context's {@link AasHttp}. */
public class AASPollingConsumer extends PollingConsumerSupport {
	private static final int WAIT_INDEFINITELY = -1;
	private static final int NO_WAIT = 0;

	public AASPollingConsumer(AASEndpoint endpoint) {
		super(endpoint);
	}

	@Override
	public AASEndpoint getEndpoint() {
		return (AASEndpoint) super.getEndpoint();
	}

	@Override
	public Exchange receive() {
		return doReceive(WAIT_INDEFINITELY);
	}

	@Override
	public Exchange receiveNoWait() {
		return doReceive(NO_WAIT);
	}

	@Override
	public Exchange receive(long timeout) {
		return doReceive(timeout);
	}

	/** Camel's timeout is in milliseconds; a failed or late read returns no exchange, never an empty value. */
	protected Exchange doReceive(long timeout) {
		Duration wait = timeout == WAIT_INDEFINITELY ? AasHttp.REQUEST_TIMEOUT : Duration.ofMillis(Math.max(1, timeout));
		try {
			Object element = JsonCodec.parseJson(AasHttp.of(getEndpoint().getCamelContext()).get(getEndpoint().getFullProxyUrl(), wait));
			// Lua receives the element's value, typed as the value feed delivers it (SPEC §5.3).
			Object decodedValue = AasValues.elementValue(element);
			if (decodedValue == null) return null;
			Exchange exchange = getEndpoint().createExchange();
			exchange.getMessage().setBody(decodedValue);
			defaultConsumerCallback(exchange, true);
			return exchange;
		} catch (IOException | RuntimeException failed) {
			getExceptionHandler().handleException(failed);
			return null;
		}
	}
}
