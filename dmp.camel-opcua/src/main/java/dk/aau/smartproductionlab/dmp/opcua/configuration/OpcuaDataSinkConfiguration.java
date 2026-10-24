/*******************************************************************************
 * Copyright (C) 2024 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.opcua.configuration;

import java.util.HashMap;

import org.apache.camel.CamelContext;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.ManagedRouteResource;
import dk.aau.smartproductionlab.dmp.core.security.BoundCredential;
import java.util.Map;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * An implementation of OpcUa Producer configuration
 *
 * @author zielstor
 *
 */
public class OpcuaDataSinkConfiguration extends DataSinkConfiguration implements ManagedRouteResource {
    protected static Logger logger = LoggerFactory.getLogger(OpcuaDataSinkConfiguration.class);

	private String serverUrl;
	private int serverPort;
    private String pathToService;
    private String nodeInformation;
    private String username;
    private String password;
    private Map<String, String> configuration = new HashMap<>();
    private BoundCredential bound;

    public OpcuaDataSinkConfiguration() {
    }

    public OpcuaDataSinkConfiguration(
            String uniqueId,
            String serverUrl,
            int serverPort,
            String pathToService,
            String nodeInformation,
            String username,
            String password,
            Map<String, String> configuration
    ) {
		super(uniqueId);

		this.serverUrl = serverUrl;
		this.serverPort = serverPort;
        this.pathToService = pathToService;
        this.nodeInformation = nodeInformation;
        this.username = username;
        this.password = password;
        this.configuration = configuration;

        if (!this.configuration.containsKey("allowedSecurityPolicies"))
            this.configuration.put("allowedSecurityPolicies", "None");

        // Read, Write and Call give up after 30 s, as HTTP and MQTT actions do (Milo's default is 60 s).

        if (!this.configuration.containsKey("requestTimeout"))

        	this.configuration.put("requestTimeout", "30000");
    }

	public String getServerUrl() {
		return serverUrl;
	}

	public void setServerUrl(String serverUrl) {
		this.serverUrl = serverUrl;
	}

	public int getServerPort() {
		return serverPort;
	}

	public void setServerPort(int serverPort) {
		this.serverPort = serverPort;
	}

    public String getPathToService() {
        return pathToService;
    }

    public void setPathToService(String pathToService) {
        this.pathToService = pathToService;
    }

    public String getNodeInformation() {
        return nodeInformation;
    }

    public void setNodeInformation(String nodeInformation) {
        this.nodeInformation = nodeInformation;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Map<String, String> getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Map<String, String> configuration) {
        this.configuration = configuration;
    }

    public String getConnectionURI() {
        return OpcuaDataSourceConfiguration.connectionUri(getServerUrl(), getServerPort(), getPathToService(),
                getNodeInformation(), boundCredential(), configuration);
    }

    private synchronized BoundCredential boundCredential() {
        if (bound == null && username != null && password != null) bound = new BoundCredential(username, password);
        return bound;
    }

    @Override
    public void bind(CamelContext context) {
        BoundCredential credential = boundCredential();
        if (credential != null) credential.bind(context);
    }

    @Override
    public synchronized void close() {
        if (bound != null) bound.close();
    }
}
