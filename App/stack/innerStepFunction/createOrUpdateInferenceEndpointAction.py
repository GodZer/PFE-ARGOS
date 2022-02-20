import aws_cdk.aws_stepfunctions as sf
import aws_cdk.aws_stepfunctions_tasks as sft
from constructs import Construct

"""
Expected input:
{
  "endpoint_configuration_name": <str>
  "username": <str>
}
"""
class CreateOrUpdateInferenceEndpointAction(sf.StateMachineFragment):
    def __init__(self, scope: Construct, id: str):
        super().__init__(scope, id)

        value_compute=sf.Pass(self, "EndpointCompute", 
            parameters={
                "endpoint_name.$": "States.Format('{}-endpoint', $.username)",
                "endpoint_configuration_name.$": "$.endpoint_configuration_name",
                "username.$": "$.username"
            })

        getEndpointTask = sft.CallAwsService(self, "getEndpointTask", 
            service="sagemaker",
            action="describeEndpoint",
            iam_resources=['*'],
            parameters={
                "EndpointName": sf.JsonPath.string_at("$.endpoint_name")
            },
            result_selector={
                "old_endpoint_config_name": sf.JsonPath.string_at("$.EndpointConfigName")
            },
            result_path="$.endpoint")

        createEndpointTask = sft.SageMakerCreateEndpoint(self, "createEndpoint",
            endpoint_config_name=sf.JsonPath.string_at("$.endpoint_configuration_name"),
            endpoint_name=sf.JsonPath.string_at("$.endpoint_name"))
        
        getEndpointTask.add_catch(createEndpointTask, result_path=sf.JsonPath.DISCARD)

        updateEndpointConfig = sft.SageMakerUpdateEndpoint(self, "updateEndpoint", 
            endpoint_config_name=sf.JsonPath.string_at("$.endpoint_configuration_name"),
            endpoint_name=sf.JsonPath.string_at("$.endpoint_name"),
            result_path=sf.JsonPath.DISCARD)

        deleteOldEndpointConfiguration = sft.CallAwsService(self, "deleteOldConfiguration",
            service="sagemaker",
            action="deleteEndpointConfig",
            iam_resources=["*"],
            parameters={
                "EndpointConfigName": sf.JsonPath.string_at("$.endpoint.old_endpoint_config_name")
            },
            result_path=sf.JsonPath.DISCARD)

        template = value_compute.next(getEndpointTask).next(updateEndpointConfig).next(deleteOldEndpointConfiguration)
        
        self._start_state=template.start_state
        self._end_states=template.end_states

    @property
    def start_state(self):
        return self._start_state

    @property
    def end_states(self):
        return self._end_states
