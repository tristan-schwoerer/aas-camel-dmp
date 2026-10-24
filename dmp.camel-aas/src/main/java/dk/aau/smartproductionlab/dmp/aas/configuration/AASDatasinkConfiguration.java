/*******************************************************************************
 * Copyright (C) 2021 the Eclipse BaSyx Authors
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
package dk.aau.smartproductionlab.dmp.aas.configuration;

import dk.aau.smartproductionlab.dmp.core.configuration.entity.DataSinkConfiguration;

/**
 * An implementation of AAS data sink configuration
 * 
 * @author haque, kammognie
 *
 */
public class AASDatasinkConfiguration extends DataSinkConfiguration {
	private String submodelEndpoint;
	private String idShortPath;
	private String elementType = "Property";

	public AASDatasinkConfiguration(String submodelEndpoint, String idShortPath, String uniqueId) {
		super(uniqueId);
		this.submodelEndpoint = submodelEndpoint;
		this.idShortPath = idShortPath;
	}

	public String getSubmodelEndpoint() {
		return submodelEndpoint;
	}

	public void setSubmodelEndpoint(String endpoint) {
		this.submodelEndpoint = endpoint;
	}

	public String getIdShortPath() {
		return idShortPath;
	}

	public void setIdShortPath(String path) {
		this.idShortPath = path;
	}

	/** The element's model type, Property or SubmodelElementCollection: how the value is encoded. */
	public String getElementType() {
		return elementType;
	}

	public void setElementType(String elementType) {
		this.elementType = elementType;
	}

	// the optional JSON field selector (valuePath) is inherited from
	// DataSinkConfiguration; this sink applies it internally (handlesValuePathInternally)

	@Override
	public boolean handlesValuePathInternally() {
		return true;
	}

	@Override
	public String getConnectionURI() {
		String endpointDefinition = "aas:";
		endpointDefinition += this.submodelEndpoint;
		endpointDefinition += "?propertyPath=" + this.idShortPath;
		if (!"Property".equals(elementType)) endpointDefinition += "&elementType=" + elementType;
		if (getValuePath() != null && !getValuePath().isBlank()) {
			endpointDefinition += "&valuePath=" + getValuePath();
		}
		return endpointDefinition;
	}
}
