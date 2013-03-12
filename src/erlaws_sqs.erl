%%-------------------------------------------------------------------
%% @author Sascha Matzke <sascha.matzke@didolo.org>
%% @doc This is an client implementation for Amazon's Simple Queue Service
%% (This is a forked version by Kazuhiro Ogura <rgoura@karesansui-project.info>)
%% @end
%%%-------------------------------------------------------------------

-module(erlaws_sqs).

%% exports
-export([new/3]).
-export([list_queues/1, list_queues/2, get_queue_url/2, create_queue/2,
	 create_queue/3, get_queue_attr/2, set_queue_attr/4, 
	 delete_queue/2, send_message/3, 
	 receive_message/2, receive_message/3, receive_message/4,
	 delete_message/3]).

%% include record definitions
-include_lib("xmerl/include/xmerl.hrl").
-include("../include/erlaws.hrl").

-define(AWS_SQS_HOST, "queue.amazonaws.com").
-define(AWS_SQS_VERSION, "2008-01-01").
-define(USE_SIGNATURE_V1, false).

new(AWS_KEY, AWS_SEC_KEY, SECURE) ->
	{?MODULE, [AWS_KEY, AWS_SEC_KEY, SECURE]}.

%% queues

%% Creates a new SQS queue with the given name. 
%%
%% SQS assigns the queue a queue URL; you must use this URL when 
%% performing actions on the queue (for more information, see 
%% http://docs.amazonwebservices.com/AWSSimpleQueueService/2007-05-01/SQSDeveloperGuide/QueueURL.html).
%%
%% Spec: create_queue(QueueName::string()) ->
%%       {ok, QueueUrl::string(), {requestId, RequestId::string()}} |
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%% 
create_queue(QueueName, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try query_request("CreateQueue", [{"QueueName", QueueName}], THIS) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=QueueUrl}|_] = 
			xmerl_xpath:string("//QueueUrl/text()", XmlDoc),
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    {ok, QueueUrl, {requestId, RequestId}}
    catch
	throw:{error, Descr, Err} ->
	    {error, Descr, Err}
    end.

%% Creates a new SQS queue with the given name and default VisibilityTimeout.
%% 
%% Spec: create_queue(QueueName::string(), VisibilityTimeout::integer()) ->
%%       {ok, QueueUrl::string(), {requestId, ReqId::string()}} |
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%% 
create_queue(QueueName, VisibilityTimeout, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_integer(VisibilityTimeout) ->
    try query_request("CreateQueue", [{"QueueName", QueueName}, 
		    {"DefaultVisibilityTimeout", integer_to_list(VisibilityTimeout)}], THIS) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=QueueUrl}|_] = 
			xmerl_xpath:string("//QueueUrl/text()", XmlDoc),
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    {ok, QueueUrl, {requestId, RequestId}}
    catch
		throw:{error, Descr, Err} ->
	    	{error, Descr, Err}
    end.
	

%% Returns a list of existing queues (QueueUrls).
%%
%% Spec: list_queues() ->
%%       {ok, [QueueUrl::string(),...], {requestId, ReqId::string()}} |
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%%
list_queues({?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try query_request("ListQueues", [], THIS) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    QueueNodes = xmerl_xpath:string("//QueueUrl/text()", XmlDoc),
		%% io:format("QueueNodes: ~p~n", [QueueNodes]),
		[#xmlText{value=RequestId}] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
		%% io:format("RequestId: ~p~n", [RequestId]),
	    {ok, [QueueUrl || #xmlText{value=QueueUrl} <- QueueNodes], {requestId, RequestId}}
    catch
		throw:{error, Descr, Err} ->
	    	{error, Descr, Err}
    end.

%% Returns a list of existing queues (QueueUrls) whose names start
%% with the given prefix
%%
%% Spec: list_queues(Prefix::string()) ->
%%       {ok, [QueueUrl::string(),...], {requestId, ReqId::string()}} |
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%%
list_queues(Prefix, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try query_request("ListQueues", [{"QueueNamePrefix", Prefix}], THIS) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    QueueNodes = xmerl_xpath:string("//QueueUrl/text()", XmlDoc),
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    {ok, [Queue || #xmlText{value=Queue} <- QueueNodes], {requestId, RequestId}}
    catch
		throw:{error, Descr, Err} ->
	    	{error, Descr, Err}
    end.
    
%% Returns the Url for a specific queue-name
%%
%% Spec: get_queue(QueueName::string()) ->
%%       {ok, QueueUrl::string(), {requestId, ReqId::string()}} |
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}} |
%%       {error, no_match}
%%
get_queue_url(QueueName, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try query_request("ListQueues", [{"QueueNamePrefix", QueueName}], THIS) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),	    
	    QueueNodes = xmerl_xpath:string("//QueueUrl/text()", XmlDoc),
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    case [Url || Url <- [Queue || #xmlText{value=Queue} <- QueueNodes],
			 string:right(Url, length(QueueName) + 1) =:= "/" ++ QueueName] of
		[] ->
		    {error, no_match};
		[QueueUrl] ->
		    {ok, QueueUrl, {requestId, RequestId}};
		[_QueueUrl|_] ->
		    throw(too_many_matches)
	    end
    catch
		throw:{error, Descr, Err} ->
	    	{error, Descr, Err}
    end.

%% Returns the attributes for the given QueueUrl
%%
%% Spec: get_queue_attr(QueueUrl::string()) ->
%%       {ok, [{"VisibilityTimeout", Timeout::integer()},
%%             {"ApproximateNumberOfMessages", Number::integer()}], {requestId, ReqId::string()}} |
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%%
get_queue_attr(QueueUrl, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try query_request(QueueUrl, "GetQueueAttributes",  
		       [{"AttributeName", "All"}], THIS) of
	{ok, Body} -> 
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    AttributeNodes = xmerl_xpath:string("//Attribute", XmlDoc),
	    AttrList = [{Key, 
			 list_to_integer(Value)} || Node <- AttributeNodes,  
			begin
			    [#xmlText{value=Key}|_] = 
				xmerl_xpath:string("./Name/text()", Node),
			    [#xmlText{value=Value}|_] =
				xmerl_xpath:string("./Value/text()", Node),
			    true
			end],
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    {ok, AttrList, {requestId, RequestId}}
    catch
	throw:{error, Descr, Err} ->
	    {error, Descr, Err}
    end.

%% This function allows you to alter the default VisibilityTimeout for
%% a given QueueUrl
%%
%% Spec: set_queue_attr(visibility_timeout, QueueUrl::string(), 
%%                      Timeout::integer()) ->
%%       {ok, {requestId, ReqId::string()}} |
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%%
set_queue_attr(visibility_timeout, QueueUrl, Timeout, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) 
  when is_integer(Timeout) ->
    try query_request(QueueUrl, "SetQueueAttributes", 
		       [{"Attribute.Name", "VisibilityTimeout"},
			{"Attribute.Value", integer_to_list(Timeout)}], THIS) of
	{ok, Body} -> 
		{XmlDoc, _Rest} = xmerl_scan:string(Body),
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    {ok, {requestId, RequestId}}
    catch
	throw:{error, Descr, Err} ->
	    {error, Descr, Err}
    end.

%% Deletes the queue identified by the given QueueUrl.
%%
%% Spec: delete_queue(QueueUrl::string(), Force::boolean()) ->
%%       {ok, {requestId, ReqId::string()}} |
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%%
delete_queue(QueueUrl, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try query_request(QueueUrl, "DeleteQueue", [], THIS) of
	{ok, Body} -> 
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
		{ok, {requestId, RequestId}}
    catch
	throw:{error, Descr, Err} ->
	    {error, Descr, Err}
    end.

%% messages

%% Sends a message to the given QueueUrl. The message must not be greater
%% that 8 Kb or the call will fail.
%%
%% Spec: send_message(QueueUrl::string(), Message::string()) ->
%%       {ok, Message::#sqs_message, {requestId, ReqId::string()}} |
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%%
send_message(QueueUrl, Message, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try query_request(QueueUrl, "SendMessage", [{"MessageBody", Message}], THIS) of
	{ok, Body} -> 
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=MessageId}|_] = 
		xmerl_xpath:string("//MessageId/text()", XmlDoc),
		[#xmlText{value=ContentMD5}|_] = 
			xmerl_xpath:string("//MD5OfMessageBody/text()", XmlDoc),
		[#xmlText{value=RequestId}|_] = xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    {ok, #sqs_message{messageId=MessageId, contentMD5=ContentMD5, body=Message}, {requestId, RequestId}}
    catch
	throw:{error, Descr, Err} ->
	    {error, Descr, Err}
    end.


%% Tries to receive a single message from the given queue.
%%
%% Spec: receive_message(QueueUrl::string()) ->
%%       {ok, [Message#sqs_message{}], {requestId, ReqId::string()}} |
%%       {ok, [], {requestId, ReqId::string()}}
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%%
receive_message(QueueUrl, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    receive_message(QueueUrl, 1, THIS).

%% Tries to receive the given number of messages (<=10) from the given queue.
%%
%% Spec: receive_message(QueueUrl::string(), NrOfMessages::integer()) ->
%%       {ok, [Message#sqs_message{}], {requestId, ReqId::string()}} |
%%       {ok, [], {requestId, ReqId::string()}}
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%%
receive_message(QueueUrl, NrOfMessages, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
	receive_message(QueueUrl, NrOfMessages, [], THIS).

%% Tries to receive the given number of messages (<=10) from the given queue, using the given VisibilityTimeout instead
%% of the queues default value.
%%
%% Spec: receive_message(QueueUrl::string(), NrOfMessages::integer(), VisibilityTimeout::integer()) ->
%%       {ok, [Message#sqs_message{}], {requestId, ReqId::string()}} |
%%       {ok, [], {requestId, ReqId::string()}}
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%%
receive_message(QueueUrl, NrOfMessages, VisibilityTimeout, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_integer(NrOfMessages) ->
	VisibilityTimeoutParam = case VisibilityTimeout of
		"" -> [];
		_ -> [{"VisibilityTimeout", integer_to_list(VisibilityTimeout)}] end,
    try query_request(QueueUrl, "ReceiveMessage", 
		     [{"MaxNumberOfMessages", integer_to_list(NrOfMessages)}] ++ VisibilityTimeoutParam, THIS) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
		[#xmlText{value=RequestId}|_] = 
		  xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    MessageNodes = xmerl_xpath:string("//Message", XmlDoc),
	    {ok, [#sqs_message{messageId=MsgId, receiptHandle=ReceiptHandle, contentMD5=ContentMD5, body=MsgBody}  || Node <- MessageNodes,
				 begin
				    [#xmlText{value=MsgId}|_] =
					 xmerl_xpath:string("./MessageId/text()", Node),
				    [#xmlText{value=MsgBody}|_] =
					 xmerl_xpath:string("./Body/text()", Node),
					[#xmlText{value=ReceiptHandle}|_] =
					 xmerl_xpath:string("./ReceiptHandle/text()", Node),
					[#xmlText{value=ContentMD5}|_] =
					 xmerl_xpath:string("./MD5OfBody/text()", Node),
				     true
				 end], {requestId, RequestId}}
    catch
		throw:{error, Descr, Err} ->
	    	{error, Descr, Err}
    end.

%% Deletes a message from a queue
%%
%% Spec: delete_message(QueueUrl::string(), RequestUrl::string()) ->
%%       {ok, {requestId, RequestId::string()}} |
%%       {error, {HTTPStatus::string, HTTPReason::string()}, {Code::string(), Message::string(), {requestId, ReqId::string()}}}
%%
delete_message(QueueUrl, ReceiptHandle, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try query_request(QueueUrl, "DeleteMessage",
		      [{"ReceiptHandle", ReceiptHandle}], THIS) of
	{ok, Body} ->
		{XmlDoc, _Rest} = xmerl_scan:string(Body),
		[#xmlText{value=RequestId}|_] = 
		  xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    {ok, {requestId, RequestId}}
    catch
	throw:{error, Descr, Err} ->
	    {error, Descr, Err}
    end.

%% internal methods

sign(Key,Data,_THIS) ->
    %%%% io:format("Sign:~n ~p~n", [Data]),
    binary_to_list(base64:encode(crypto:sha_mac(Key,Data))).

query_request(Action, Parameters, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, SECURE]}=THIS) ->
	Prefix = case SECURE of
			true -> "https://";
			_ -> "http://"
		end,
	query_request(Prefix ++ ?AWS_SQS_HOST ++ "/", Action, Parameters, THIS).

query_request(Url, Action, Parameters, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when ?USE_SIGNATURE_V1 ->
    query_requestV1(Url, Action, Parameters, THIS);
query_request(Url, Action, Parameters, {?MODULE, [AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    Timestamp = lists:flatten(erlaws_util:get_timestamp()),
    Params = [{"Action", Action}, {"AWSAccessKeyId", AWS_KEY}, {"Timestamp", Timestamp}]
	++Parameters ++ [{"Version", ?AWS_SQS_VERSION}],
    Result = mkReq(Url, Params, THIS),
    case Result of
	{ok, _Status, Body} ->
	    {ok, Body};
 	{error, {_Proto, Code, Reason}, Body} ->
	    throw({error, {integer_to_list(Code), Reason}, mkErr(Body, THIS)})
    end.

mkReq(Url, Params, {?MODULE, [_AWS_KEY, AWS_SEC_KEY, _SECURE]}=THIS) ->
    QueryParams = [{"SignatureVersion", "2"}|[{"SignatureMethod", "HmacSHA1"}|Params]],
    ParamsString = erlaws_util:mkEnumeration([ erlaws_util:url_encode(Key) ++ "=" ++ erlaws_util:url_encode(Value) ||
						 {Key, Value} <- lists:keysort(1, QueryParams)],
					     "&"),
    {_, _, Host, _, Path, _} = http_uri:parse(Url),
    StringToSign = "POST\n" ++ string:to_lower(Host) ++ "\n" ++ Path ++ "\n" ++ ParamsString,
    Signature = sign(AWS_SEC_KEY, StringToSign, THIS),
    SignatureString = "&Signature=" ++ erlaws_util:url_encode(Signature),
    PostData = ParamsString ++ SignatureString,
    %io:format("~p~n~s~n~s~n", [Params, Url, PostData]),
    Request = {Url, [], "application/x-www-form-urlencoded", PostData},
    HttpOptions = [{autoredirect, true}],
    Options = [{sync,true}, {body_format, binary}],
    {ok, {Status, _ReplyHeaders, Body}} =
	httpc:request(post, Request, HttpOptions, Options),
    case Status of 
	{_, 200, _} -> {ok, Status, binary_to_list(Body)};
	{_, _, _} -> {error, Status, binary_to_list(Body)}
    end.

mkErr(Xml, _THIS) ->
    {XmlDoc, _Rest} = xmerl_scan:string( Xml ),
    [#xmlText{value=ErrorCode}|_] = xmerl_xpath:string("//Error/Code/text()", 
						       XmlDoc),
    ErrorMessage = 
	case xmerl_xpath:string("//Error/Message/text()", XmlDoc) of
	    [] -> "";
	    [EMsg|_] -> EMsg#xmlText.value
	end,
    RequestId = case xmerl_xpath:string("//RequestID/text()", XmlDoc) of
		    [#xmlText{value=Value}|_] -> Value;
		    _ ->
			case xmerl_xpath:string("//RequestId/text()", XmlDoc) of
			    [#xmlText{value=Value}|_] -> Value;
			    _ -> ""
			end
		end,
    {ErrorCode, ErrorMessage, {requestId, RequestId}}.


%%% Erlang/OTP Releases before R14A can't handle Signature ver.2 + SSL request
%%% implemented above (plain requests work fine). Signature ver.1 based code are
%%% left below for compatibility.

query_requestV1(Url, Action, Parameters, {?MODULE, [AWS_KEY, AWS_SEC_KEY, _SECURE]}=THIS) ->
	%% io:format("query_request: ~p ~p ~p~n", [Url, Action, Parameters]),
    Timestamp = lists:flatten(erlaws_util:get_timestamp()),
	SignParams = [{"Action", Action}, {"AWSAccessKeyId", AWS_KEY}, {"Timestamp", Timestamp}] ++
				 Parameters ++ [{"SignatureVersion", "1"}, {"Version", ?AWS_SQS_VERSION}],
	StringToSign = erlaws_util:mkEnumeration([Param++Value || {Param, Value} <- lists:sort(fun (A, B) -> 
		{KeyA, _} = A,
		{KeyB, _} = B,
		string:to_lower(KeyA) =< string:to_lower(KeyB) end, 
		SignParams)], ""),
	%% io:format("StringToSign: ~p~n", [StringToSign]),
    Signature = sign(AWS_SEC_KEY, StringToSign, THIS),
	%% io:format("Signature: ~p~n", [Signature]),
    FinalQueryParams = SignParams ++
			[{"Signature", Signature}],
    Result = mkReqV1(get, Url, [], FinalQueryParams, "", "", THIS),
    case Result of
	{ok, _Status, Body} ->
	    {ok, Body};
	{error, {_Proto, Code, Reason}, Body} ->
	    throw({error, {integer_to_list(Code), Reason}, mkErr(Body, THIS)})
    end.

mkReqV1(Method, PreUrl, Headers, QueryParams, ContentType, ReqBody, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    %%%% io:format("QueryParams:~n ~p~nHeaders:~n ~p~nUrl:~n ~p~n", 
    %%      [QueryParams, Headers, PreUrl]),
    Url = PreUrl ++ erlaws_util:queryParams( QueryParams ),
    %% io:format("RequestUrl:~n ~p~n", [Url]),
    Request = case Method of
 		  get -> { Url, Headers };
 		  put -> { Url, Headers, ContentType, ReqBody }
 	      end,

    HttpOptions = [{autoredirect, true}],
    Options = [ {sync,true}, {headers_as_is,true}, {body_format, binary} ],
    {ok, {Status, _ReplyHeaders, Body}} = 
	http:request(Method, Request, HttpOptions, Options, THIS),
    %% io:format("Response:~n ~p~n", [binary_to_list(Body)]),
	%% io:format("Status: ~p~n", [Status]),
    case Status of 
	{_, 200, _} -> {ok, Status, binary_to_list(Body)};
	{_, _, _} -> {error, Status, binary_to_list(Body)}
    end.
