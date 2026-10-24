package dk.aau.smartproductionlab.dmp.aas.operation;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.eclipse.digitaltwin.aas4j.v3.model.SubmodelElement;

/**
 * Per-request AAS Operation context; target connector settings remain opaque. Every id comes from the
 * compiled mapping: nothing defaults to the action's name.
 */
public final class OperationContext {
	public final String action;
	public final String responseLuaUri;
	public final String commandSinkUri;
	public final Map<String, String> parameterTypes;
	public final Map<String, String> responseTypes;
	public final Map<String, String> callerResponseTypes;
	public final List<String> callerOutputOrder;
	/** Declared caller input and InOut variables, by idShort; they type the request values. */
	public final Map<String, SubmodelElement> callerInputTemplates;
	/** Declared caller InOut and output variables, by idShort; the reply is rebuilt from them. */
	public final Map<String, SubmodelElement> callerOutputTemplates;
	/** SourceId -> path in the caller's flattened OperationVariable values ({@code $}: the whole request). */
	public final Map<String, String> callerInputSourcePaths;
	/** Response SinkId -> path in the caller's output-variable object. */
	public final Map<String, String> callerReplyBindings;
	/** The ResponseSink that answers the caller; null when none does (the Operation answers with no outputs). */
	public final String callerReplySinkId;
	/** The command's SinkIds, assembled into one affordance payload. */
	public final List<String> commandSinkIds;
	public final Map<String, String> commandSinkValuePaths;
	public final boolean explicitResponseMapping;
	public final Set<String> parameterRequired;
	public final Set<String> responseRequired;
	/** The Operation writes a property instead of invoking an action; the written value is its reply. */
	public final boolean propertyWrite;
	/** A written property's value is sent as JSON text (HTTP and MQTT with a JSON schema). */
	public final boolean jsonPayload;
	/** The connector's invocation options ({@code SynchronousInvocationAdapter.InvocationSchema}). */
	public final Map<String, Object> targetOptions;

	private OperationContext(Builder builder) {
		this.action = Objects.requireNonNull(builder.action, "action");
		this.responseLuaUri = builder.responseLuaUri;
		this.commandSinkUri = Objects.requireNonNull(builder.commandSinkUri, "commandSinkUri");
		this.parameterTypes = Map.copyOf(builder.parameterTypes);
		this.responseTypes = Map.copyOf(builder.responseTypes);
		this.callerResponseTypes = Map.copyOf(builder.callerResponseTypes);
		this.callerOutputOrder = List.copyOf(builder.callerOutputOrder);
		this.callerInputTemplates = Map.copyOf(builder.callerInputTemplates);
		this.callerOutputTemplates = Map.copyOf(builder.callerOutputTemplates);
		this.callerInputSourcePaths = Map.copyOf(builder.callerInputSourcePaths);
		this.callerReplyBindings = Map.copyOf(builder.callerReplyBindings);
		this.callerReplySinkId = builder.callerReplySinkId;
		this.commandSinkIds = List.copyOf(builder.commandSinkIds);
		this.commandSinkValuePaths = Map.copyOf(builder.commandSinkValuePaths);
		this.explicitResponseMapping = builder.explicitResponseMapping;
		this.parameterRequired = Set.copyOf(builder.parameterRequired);
		this.responseRequired = Set.copyOf(builder.responseRequired);
		this.propertyWrite = builder.propertyWrite;
		this.jsonPayload = builder.jsonPayload;
		this.targetOptions = Map.copyOf(builder.targetOptions);
		if (callerInputSourcePaths.isEmpty())
			throw new IllegalArgumentException("operation " + action + " has no Source on the Operation");
		if (commandSinkIds.isEmpty())
			throw new IllegalArgumentException("operation " + action + " has no command Sink");
	}

	/** {@code action} names the Operation in messages and {@code meta}; {@code commandSinkUri} is the device. */
	public static Builder builder(String action, String commandSinkUri) {
		return new Builder(action, commandSinkUri);
	}

	public static final class Builder {
		private final String action;
		private final String commandSinkUri;
		private String responseLuaUri;
		private Map<String, String> parameterTypes = Map.of();
		private Map<String, String> responseTypes = Map.of();
		private Map<String, String> callerResponseTypes = Map.of();
		private List<String> callerOutputOrder = List.of();
		private Map<String, SubmodelElement> callerInputTemplates = Map.of();
		private Map<String, SubmodelElement> callerOutputTemplates = Map.of();
		private Map<String, String> callerInputSourcePaths = Map.of();
		private Map<String, String> callerReplyBindings = Map.of();
		private String callerReplySinkId;
		private List<String> commandSinkIds = List.of();
		private Map<String, String> commandSinkValuePaths = Map.of();
		private boolean explicitResponseMapping;
		private List<String> parameterRequired = List.of();
		private List<String> responseRequired = List.of();
		private boolean propertyWrite;
		private boolean jsonPayload;
		private Map<String, Object> targetOptions = Map.of();

		private Builder(String action, String commandSinkUri) {
			this.action = action;
			this.commandSinkUri = commandSinkUri;
		}

		public Builder responseLuaUri(String uri) { this.responseLuaUri = uri; return this; }
		public Builder parameterTypes(Map<String, String> types) { this.parameterTypes = types; return this; }
		public Builder responseTypes(Map<String, String> types) { this.responseTypes = types; return this; }
		public Builder callerResponseTypes(Map<String, String> types) { this.callerResponseTypes = types; return this; }
		public Builder callerOutputOrder(List<String> order) { this.callerOutputOrder = order; return this; }
		public Builder callerInputTemplates(Map<String, SubmodelElement> templates) { this.callerInputTemplates = templates; return this; }
		public Builder callerOutputTemplates(Map<String, SubmodelElement> templates) { this.callerOutputTemplates = templates; return this; }
		public Builder callerInputSourcePaths(Map<String, String> paths) { this.callerInputSourcePaths = paths; return this; }
		public Builder callerReplyBindings(Map<String, String> bindings) { this.callerReplyBindings = bindings; return this; }
		public Builder callerReplySinkId(String sinkId) { this.callerReplySinkId = sinkId; return this; }
		public Builder commandSinkIds(List<String> sinkIds) { this.commandSinkIds = sinkIds; return this; }
		public Builder commandSinkValuePaths(Map<String, String> paths) { this.commandSinkValuePaths = paths; return this; }
		public Builder explicitResponseMapping(boolean explicit) { this.explicitResponseMapping = explicit; return this; }
		public Builder parameterRequired(List<String> names) { this.parameterRequired = names; return this; }
		public Builder responseRequired(List<String> names) { this.responseRequired = names; return this; }
		public Builder propertyWrite(boolean write) { this.propertyWrite = write; return this; }
		public Builder jsonPayload(boolean json) { this.jsonPayload = json; return this; }
		public Builder targetOptions(Map<String, Object> options) { this.targetOptions = options; return this; }

		public OperationContext build() { return new OperationContext(this); }
	}
}
