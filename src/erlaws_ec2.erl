-module(erlaws_ec2).
-author	(dieu).

-include_lib("xmerl/include/xmerl.hrl").

-export ([new/3, start_instances/2, run_instances/14, stop_instances/3, terminate_instances/2, describe_instances/1, describe_instances/2]).

-define (AWS_EC2_HOST, "ec2.amazonaws.com").
-define	(AWS_EC2_VERSION, "2009-11-30").

new(AWS_KEY, AWS_SEC_KEY, SECURE) ->
	{?MODULE, [AWS_KEY, AWS_SEC_KEY, SECURE]}.

% API
describe_instances({?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
	describe_instances([], THIS).

describe_instances(InstanceIds, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
	Fun = fun(AIM, [Inc, List]) -> [Inc + 1, lists:append(List, [{lists:flatten(io_lib:format("InstanceId.~p", [Inc])), AIM}])] end,
	[_, Params] = lists:foldl(Fun, [1, []], InstanceIds),
	try query_request("DescribeInstances", Params, THIS) of
		{ok, Body} ->
			{ResponseXML, _Rest} = xmerl_scan:string(Body),
			Response = lists:foldl(
				fun(Elem, ACC) ->
					[#xmlText{value = ReservationId} | _] = xmerl_xpath:string("//reservationId/text()", Elem),
					InstanseSet = lists:foldl(
						fun(Item, Items) ->
							[#xmlText{value = InstanceId} | _] = xmerl_xpath:string("//instanceId/text()", Item),
							[#xmlText{value = InstenceState} | _] = xmerl_xpath:string("//instanceState/name/text()", Item),
							[#xmlText{value = PrivateDnsName} | _] = xmerl_xpath:string("//privateDnsName/text()", Item),
							[#xmlText{value = DnsName} | _] = xmerl_xpath:string("//dnsName/text()", Item),
							[#xmlText{value = Type} | _] = xmerl_xpath:string("//instanceType/text()", Item),
							Items ++ [{InstanceId, InstenceState, PrivateDnsName, DnsName, Type}]
						end, 
					[], xmerl_xpath:string("//instancesSet/item", Elem)),
					ACC ++ [{ReservationId, InstanseSet}]
				end,
			[], xmerl_xpath:string("//DescribeInstancesResponse/reservationSet/item", ResponseXML)),
			Response
	catch
		throw:{error, Descr} ->
		    {error, Descr}
	end.

terminate_instances(InstanceId, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
	try query_request("TerminateInstances", [{"InstanceId", InstanceId}], THIS) of
		{ok, Body} ->
			{ResponseXML, _Rest} = xmerl_scan:string(Body),
			Response = lists:foldl(
				fun(Elem, ACC) ->
					[#xmlText{value = Id} | _] = xmerl_xpath:string("//instanceId/text()", Elem),
					[#xmlText{value = InstanceState} | _] = xmerl_xpath:string("//shutdownState/name/text()", Elem),
					[#xmlText{value = PreState} | _] = xmerl_xpath:string("//previousState/name/text()", Elem),
					ACC ++ [{Id, InstanceState, PreState}]
				end,
			[], xmerl_xpath:string("//TerminateInstancesResponse/instancesSet/item", ResponseXML)),
			Response
	catch
		throw:{error, Descr} ->
		    {error, Descr}
	end.

stop_instances(InstanceId, Force, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
	try query_request("StopInstances", [{"InstanceId", InstanceId}, {"Force", Force}], THIS) of
		{ok, Body} ->
			{ResponseXML, _Rest} = xmerl_scan:string(Body),
			Response = lists:foldl(
				fun(Elem, ACC) ->
					[#xmlText{value = Id} | _] = xmerl_xpath:string("//instanceId/text()", Elem),
					[#xmlText{value = InstanceState} | _] = xmerl_xpath:string("//currentState/name/text()", Elem),
					[#xmlText{value = PreState} | _] = xmerl_xpath:string("//previousState/name/text()", Elem),
					ACC ++ [{Id, InstanceState, PreState}]
				end,
			[], xmerl_xpath:string("//StopInstancesResponse/instancesSet/item", ResponseXML)),
			Response
	catch
		throw:{error, Descr} ->
		    {error, Descr}
	end.

start_instances(InstanceIds, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
	Fun = fun(AIM, [Inc, List]) -> [Inc + 1, lists:append(List, [{lists:flatten(io_lib:format("InstanceId.~p", [Inc])), AIM}])] end,
	[_, Params] = lists:foldl(Fun, [1, []], InstanceIds),
	try query_request("StartInstances", Params, THIS) of
		{ok, Body} ->
			{ResponseXML, _Rest} = xmerl_scan:string(Body),
			Response = lists:foldl(
				fun(Elem, ACC) ->
					[#xmlText{value = Id} | _] = xmerl_xpath:string("//instanceId/text()", Elem),
					[#xmlText{value = InstanceState} | _] = xmerl_xpath:string("//currentState/name/text()", Elem),
					[#xmlText{value = PreState} | _] = xmerl_xpath:string("//previousState/name/text()", Elem),
					ACC ++ [{Id, InstanceState, PreState}]
				end,
			[], xmerl_xpath:string("//StartInstancesResponse/instancesSet/item", ResponseXML)),
			Response
	catch
		throw:{error, Descr} ->
		    {error, Descr}
	end.

%InstanceType	Valid Values: m1.small | m1.large | m1.xlarge | c1.medium | c1.xlarge | m2.xlarge | m2.2xlarge | m2.4xlarge	
run_instances(ImageId, MinCount, MaxCount, KeyName, SecurityGroups, AddressingType, InstanceType,
							KernelId, RamdiskId, Monitoring, SubnetId, DisableApiTermination, InstanceInitiatedShutdownBehavior,
							{?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
	Fun = fun(Group, [Inc, List]) -> [Inc + 1, lists:append(List, [{lists:flatten(io_lib:format("SecurityGroup.~p", [Inc])), Group}])] end,
	[_, SecGroups] = lists:foldl(Fun, [1, []], SecurityGroups),
	try query_request("RunInstances", [{"ImageId", ImageId}, {"MinCount", lists:flatten(io_lib:format("~p", [MinCount]))}, {"KeyName", KeyName}] ++ SecGroups ++
									  [{"AddressingType", AddressingType}, {"InstanceType", InstanceType}, {"KernelId", KernelId}, {"RamdiskId", RamdiskId}, {"Monitoring", Monitoring},
									   {"SubnetId", SubnetId}, {"DisableApiTermination", DisableApiTermination}, {"InstanceInitiatedShutdownBehavior", InstanceInitiatedShutdownBehavior}, {"MaxCount", lists:flatten(io_lib:format("~p", [MaxCount]))}],
							   THIS) of
		{ok, Body} ->
			{ResponseXML, _Rest} = xmerl_scan:string(Body),
			Response = lists:foldl(
				fun(Elem, ACC) ->
					[#xmlText{value = Id} | _] = xmerl_xpath:string("//instanceId/text()", Elem),
					[#xmlText{value = InstanceState} | _] = xmerl_xpath:string("//instanceState/name/text()", Elem),
					[#xmlText{value = Type} | _] = xmerl_xpath:string("//instanceType/text()", Elem),
					[#xmlText{value = Zone} | _] = xmerl_xpath:string("//placement/availabilityZone", Elem),
					ACC ++ [{Id, InstanceState, Type, Zone}]
				end,
			[], xmerl_xpath:string("//RunInstancesResponse/instancesSet/item", ResponseXML)),
			Response
	catch
		throw:{error, Descr} ->
		    {error, Descr}
	end.
	
sign (Key, Data, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}) ->
    binary_to_list( base64:encode( crypto:sha_mac(Key,Data) ) ).

query_request(Action, Parameters, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, SECURE]}=THIS) ->
	case SECURE of 
			true -> Prefix = "https://";
			_ -> Prefix = "http://"
	end,
	query_request(Prefix ++ ?AWS_EC2_HOST ++ "/", Action, Parameters, THIS).

query_request(Url, Action, Parameters, {?MODULE, [AWS_KEY, AWS_SEC_KEY, _SECURE]}=THIS) ->
    Timestamp = lists:flatten(erlaws_util:get_timestamp()),
	SignParams = [{"Action", Action}, {"AWSAccessKeyId", AWS_KEY}, {"Timestamp", Timestamp}] ++
				 Parameters ++ [{"SignatureVersion", "1"}, {"Version", ?AWS_EC2_VERSION}],
	StringToSign = erlaws_util:mkEnumeration([Param++Value || {Param, Value} <- lists:sort(fun (A, B) -> 
		{KeyA, _} = A,
		{KeyB, _} = B,
		string:to_lower(KeyA) =< string:to_lower(KeyB) end, 
		SignParams)], ""),
    Signature = sign(AWS_SEC_KEY, StringToSign, THIS),
    FinalQueryParams = SignParams ++
			[{"Signature", Signature}],
    Result = mkReq(get, Url, [], FinalQueryParams, "", "", THIS),
    case Result of
	{ok, _Status, Body} ->
	    {ok, Body};
	{error, {_Proto, Code, Reason}, Body} ->
	    throw({error, {integer_to_list(Code), Reason}, mkErr(Body, THIS)})
    end.

mkReq(Method, PreUrl, Headers, QueryParams, ContentType, ReqBody, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}) ->
    Url = PreUrl ++ erlaws_util:queryParams( QueryParams ),
    Request = case Method of
 		  			get -> { Url, Headers };
 		  			put -> { Url, Headers, ContentType, ReqBody }
 	end,

    HttpOptions = [{autoredirect, true}],
    Options = [ {sync,true}, {headers_as_is,true}, {body_format, binary} ],
    {ok, {Status, _ReplyHeaders, Body}} = httpc:request(Method, Request, HttpOptions, Options),
    case Status of 
		{_, 200, _} -> {ok, Status, binary_to_list(Body)};
		{_, _, _} -> {error, Status, binary_to_list(Body)}
    end.	

mkErr(Xml, _THIS) ->
    {XmlDoc, _Rest} = xmerl_scan:string( Xml ),
    [#xmlText{value=ErrorCode}|_] = xmerl_xpath:string("//Error/Code/text()", XmlDoc),
    ErrorMessage = 
	case xmerl_xpath:string("//Error/Message/text()", XmlDoc) of
	    [] -> "";
	    [EMsg|_] -> EMsg#xmlText.value
	end,
    [#xmlText{value=RequestId}|_] = xmerl_xpath:string("//RequestID/text()", XmlDoc),
    {ErrorCode, ErrorMessage, {requestId, RequestId}}.
