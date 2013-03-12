%%%-------------------------------------------------------------------
%%% File    : erlaws_s3.erl
%%% Author  : Sascha Matzke <sascha.matzke@didolo.org>
%%% Description : Amazon S3 client library
%%%
%%% Created : 25 Dec 2007 by Sascha Matzke <sascha.matzke@didolo.org>
%%%-------------------------------------------------------------------

-module(erlaws_s3).

%% API
-export([new/3]).
-export([list_buckets/1, create_bucket/2, create_bucket/3, delete_bucket/2]).
-export([list_contents/2, list_contents/3, put_object/6, put_file/6, get_object/3]).
-export([info_object/3, delete_object/3]).
-export([initiate_mp_upload/5, complete_mp_upload/5, abort_mp_upload/4, upload_part/7]).

%% include record definitions
-include_lib("xmerl/include/xmerl.hrl").
-include_lib("kernel/include/file.hrl").
-include("../include/erlaws.hrl").

%% macro definitions
-define(AWS_S3_HOST, "s3.amazonaws.com").
-define(NR_OF_RETRIES, 3).
-define(CALL_TIMEOUT, indefinite).
-define(S3_REQ_ID_HEADER, "x-amz-request-id").
-define(PREFIX_XPATH, "//CommonPrefixes/Prefix/text()").
-define(CHUNK_SIZE, 8 * 1024).

new(AWS_KEY, AWS_SEC_KEY, SECURE) ->
	{?MODULE, [AWS_KEY, AWS_SEC_KEY, SECURE]}.

%% Returns a list of all of the buckets owned by the authenticated sender 
%% of the request.
%%
%% Spec: list_buckets() -> 
%%       {ok, Buckets::[Name::string()]} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
list_buckets({?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest(get, "", "", [], [], [], <<>>, THIS) of
	{ok, Headers, Body} -> 
	    {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Body)),
	    TextNodes       = xmerl_xpath:string("//Bucket/Name/text()", XmlDoc),
	    BExtr = fun (#xmlText{value=T}) -> T end,
		RequestId = case lists:keytake("x-amz-request-id", 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
	    {ok, [BExtr(Node) || Node <- TextNodes], {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Creates a new bucket. Not every string is an acceptable bucket name. 
%% See http://docs.amazonwebservices.com/AmazonS3/2006-03-01/UsingBucket.html
%% for information on bucket naming restrictions.
%% 
%% Spec: create_bucket(Bucket::string()) ->
%%       {ok, Bucket::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
create_bucket(Bucket, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest(put, Bucket, "", [], [], [], <<>>, THIS) of
	{ok, Headers, _Body} -> 
	    RequestId = case lists:keytake("x-amz-request-id", 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
		{ok, Bucket, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Creates a new bucket with a location constraint (EU). 
%%
%% *** Be aware that Amazon applies a different pricing for EU buckets *** 
%%
%% Not every string is an acceptable bucket name. 
%% See http://docs.amazonwebservices.com/AmazonS3/2006-03-01/UsingBucket.html
%% for information on bucket naming restrictions.
%% 
%% Spec: create_bucket(Bucket::string(), eu) ->
%%       {ok, Bucket::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
create_bucket(Bucket, eu, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    create_bucket(Bucket, 'EU', THIS);
%% Creates a new bucket with a location constraint.
%% ex) create_bucket("bucket", 'ap-southeast-1')
%%
%% Spec: create_bucket(Bucket::string(), Region::atom()) ->
%%       {ok, Bucket::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
create_bucket(Bucket, Region, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    LCfgStr = io_lib:format("<CreateBucketConfiguration>
                  <LocationConstraint>~s</LocationConstraint>
             </CreateBucketConfiguration>", [Region]),
    LCfg = list_to_binary(LCfgStr),
    try genericRequest(put, Bucket, "", [], [], [], LCfg, THIS) of
	{ok, Headers, _Body} ->
		RequestId = case lists:keytake("x-amz-request-id", 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
	    {ok, Bucket, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Deletes a bucket. 
%% 
%% Spec: delete_bucket(Bucket::string()) ->
%%       {ok} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
delete_bucket(Bucket, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest(delete, Bucket, "", [], [], [], <<>>, THIS) of
	{ok, Headers, _Body} ->
	    RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
		{ok, {requestId, RequestId}}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Lists the contents of a bucket.
%%
%% Spec: list_contents(Bucket::string()) ->
%%       {ok, #s3_list_result{isTruncated::boolean(),
%%                         keys::[#s3_object_info{}],
%%                         prefix::[string()]}} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
list_contents(Bucket, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    list_contents(Bucket, [], THIS).

%% Lists the contents of a bucket.
%%
%% Spec: list_contents(Bucket::string(), Options::[{atom(), 
%%                     (integer() | string())}]) ->
%%       {ok, #s3_list_result{isTruncated::boolean(),
%%                         keys::[#s3_object_info{}],
%%                         prefix::[string()]}} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Options -> [{prefix, string()}, {marker, string()},
%%	             {max_keys, integer()}, {delimiter, string()}]
%%
list_contents(Bucket, Options, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Options) ->
    QueryParameters = [makeParam(X, THIS) || X <- Options],
    try genericRequest(get, Bucket, "", QueryParameters, [], [], <<>>, THIS) of
	{ok, Headers, Body} -> 
	    {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Body)),
	    [Truncated| _Tail] = xmerl_xpath:string("//IsTruncated/text()", 
						    XmlDoc),
	    ContentNodes = xmerl_xpath:string("//Contents", XmlDoc),
	    KeyList = [extractObjectInfo(Node, THIS) || Node <- ContentNodes],
	    PrefixList = [Node#xmlText.value || 
			     Node <- xmerl_xpath:string(?PREFIX_XPATH, XmlDoc)],
		RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,			
	    {ok, #s3_list_result{isTruncated=case Truncated#xmlText.value of
					      "true" -> true;
					      _ -> false end, 
			      keys=KeyList, prefixes=PrefixList}, {requestId, RequestId}}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Uploads data for key. Backwards-compatible version.
%%
%% Spec: put_object(Bucket::string(), Key::string(), Data::binary(),
%%                  ContentType::string(), 
%%                  Metadata::[{Key::string(), Value::string()}]) ->
%%       {ok, #s3_object_info(key=Key::string(), size=Size::integer())} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
put_object(Bucket, Key, Data, ContentType, Metadata, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_integer(hd(ContentType)) ->
    put_object(Bucket, Key, Data, [{"Content-Type", ContentType}], Metadata, THIS);

%% Uploads data for key. More general version.
%%
%% Spec: put_object(Bucket::string(), Key::string(), Data::binary(),
%%                  HTTPHeaders::[{Key::string(), Value::string()}]
%%                  Metadata::[{Key::string(), Value::string()}]) ->
%%       {ok, #s3_object_info(key=Key::string(), size=Size::integer()), ReqId::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%% EXAMPLE:
%% S3 = ?MODULE:new(...),	% Fill it according to your preferences.
%% S3:put_object("someBucket", "filename.js", <<"...">>, [{"Content-Type", "application/x-javascript; charset=\"utf-8\""},{"Cache-Control", "max-age=86400"},{"x-amz-acl", "public-read"}], [{"name", "metavalue"}]).
%% S3:put_object("someBucket", "filename.mp4", <<"...">>, [{"Content-Type", "video/mp4"}, {"x-amz-storage-class", "REDUCED_REDUNDANCY"}], []).
%%
put_object(Bucket, Key, Data, HTTPHeaders, Metadata, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest(put, Bucket, Key, [], Metadata, HTTPHeaders, Data, THIS) of
	{ok, Headers, _Body} -> 
	    {ok,
	     #s3_object_info{key=Key, size=iolist_size(Data),
			     etag=proplists:get_value("etag", Headers)},
	     {requestId, proplists:get_value(?S3_REQ_ID_HEADER, Headers, "")}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% @doc
%% Initiates multipart upload.
%% @end
%% -spec initiate_mp_upload(Bucket::string(), Key::string(),
%%			 HTTPHeaders::[{string(), string()}],
%%			 Metadata::[{string(), string()}]) ->
%%			    {ok, UploadId::string(), ReqId::string()}.
initiate_mp_upload(Bucket, Key, HTTPHeaders, Metadata, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest(post, Bucket, Key, [{"uploads", ""}],
		       Metadata, HTTPHeaders, <<>>, THIS) of
	{ok, Headers, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(binary_to_list(Body)),
	    [_XBucket|_] = xmerl_xpath:string("/InitiateMultipartUploadResult/Bucket/text()", XmlDoc),
	    [_XKey|_] = xmerl_xpath:string("/InitiateMultipartUploadResult/Key/text()", XmlDoc),
	    [XUploadId|_] = xmerl_xpath:string("/InitiateMultipartUploadResult/UploadId/text()", XmlDoc),
	    {ok, XUploadId#xmlText.value, proplists:get_value(?S3_REQ_ID_HEADER, Headers, "")}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% @doc
%% Completes multipart upload.
%% @end
%% -spec complete_mp_upload(Bucket::string(), Key::string(), UploadId::string(),
%% 			 [{PartNum::integer(), ETag::string()}]) -> ok.
complete_mp_upload(Bucket, Key, UploadId, Parts, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    F = fun({PartNum, ETag}) ->
		{'Part', [],
		 [
		  {'PartNumber', [integer_to_list(PartNum)]},
		  {'ETag', [ETag]}
		 ]
		}
	end,
    Req = {'CompleteMultipartUpload', [], [F(X) || X <- Parts]},
    XMLReqBody = xmerl:export_simple([Req], xmerl_xml),
    try genericRequest(post, Bucket, Key,
		       [{"uploadId", UploadId}], [], [], XMLReqBody, THIS) of
	{ok, _Headers, _Body} -> ok
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% @doc
%% Aborts multipart upload.
%% @end
%% -spec abort_mp_upload(Bucket::string(), Key::string(), UploadId::string()) ->
%%			      ok.
abort_mp_upload(Bucket, Key, UploadId, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest(delete, Bucket, Key,
		       [{"uploadId", UploadId}], [], [], <<>>, THIS) of
	{ok, _Headers, _Body} -> ok
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% @doc
%% Uploads a part in multipart upload.
%% @end
%% -spec upload_part(Bucket::string(), Key::string(), PartNum::integer(),
%%		  UploadId::string(), Data::binary(),
%%		  HTTPHeaders::[{string(), string()}]) -> ok.
upload_part(Bucket, Key, PartNum, UploadId, Data, HTTPHeaders, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest(put, Bucket, Key,
		       [{"partNumber", integer_to_list(PartNum)},
			{"uploadId", UploadId}],
		       [], HTTPHeaders, Data, THIS) of
	{ok, Headers, _Body} ->
	    {ok,
	     #s3_object_info{key=Key, size=iolist_size(Data),
			     etag=proplists:get_value("etag", Headers)},
	     {requestId, proplists:get_value(?S3_REQ_ID_HEADER, Headers, "")}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

put_file(Bucket, Key, FileName, ContentType, Metadata, {?MODULE, [AWS_KEY, AWS_SEC_KEY, _SECURE]}=THIS) ->
    Date = httpd_util:rfc1123_date(erlang:localtime()),
    {FileSize, File} = openAndGetFileSize(FileName, THIS),
    Headers = 
        buildContentHeaders(FileSize, THIS) ++
	buildMetadataHeaders(Metadata, THIS),
    Signature = sign(AWS_SEC_KEY,
                     stringToSign("PUT", "", ContentType, Date,
                                  Bucket, Key, Headers, THIS), THIS),
    FinalHeaders = [ {"Authorization", "AWS " ++ AWS_KEY ++ ":" ++ Signature },
		     {"Host", buildHost(Bucket, THIS) },
		     {"Date", Date },
		     {"Content-Type", ContentType}
		     | Headers ],
    Payload = 
        lists:append(
          ["PUT /", Key, " HTTP/1.1\n",
           lists:flatten([lists:append([K, ": ", V, "\n"]) || 
                             {K, V} <- lists:reverse(FinalHeaders)]),
           "\n"]),
    {ok, Socket} = gen_tcp:connect(?AWS_S3_HOST, 80, 
                                   [binary, {active, false}, {packet, 0}]),
    gen_tcp:send(Socket, list_to_binary(Payload)),
    sendData(Socket, File, THIS),
    gen_tcp:close(Socket),
    file:close(File).
       
%% Retrieves the data associated with the given key.
%% 
%% Spec: get_object(Bucket::string(), Key::string()) ->
%%       {ok, Data::binary()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
get_object(Bucket, Key, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest(get, Bucket, Key, [], [], [], <<>>, THIS) of
	{ok, Headers, Body} -> 
		RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
		{ok, Body, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.
    
%% Returns the metadata associated with the given key.
%%
%% Spec: info_object(Bucket::string(), Key::string()) ->
%%       {ok, [{Key::string(), Value::string()},...], {requestId, ReqId::string()}} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
info_object(Bucket, Key, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest(head, Bucket, Key, [], [], [], <<>>, THIS) of
	{ok, Headers, _Body} ->
	    io:format("Headers: ~p~n", [Headers]),
		MetadataList = [{string:substr(MKey, 12), Value} || {MKey, Value} <- Headers, string:str(MKey, "x-amz-meta") == 1],
		RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
		{ok, MetadataList, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Delete the given key from bucket.
%% 
%% Spec: delete_object(Bucket::string(), Key::string()) ->
%%       {ok} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
delete_object(Bucket, Key, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest(delete, Bucket, Key, [], [], [], <<>>, THIS) of
	{ok, Headers, _Body} ->
		RequestId = case lists:keytake(?S3_REQ_ID_HEADER, 1, Headers) of
			{value, {_, ReqId}, _} -> ReqId;
			_ -> "" end,
	    {ok, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

isAmzHeader( Header, _THIS ) -> lists:prefix("x-amz-", Header).

aggregateValues ({K,V}, [{K,L}|T], _THIS) -> [{K,[V|L]}|T];
aggregateValues ({K,V}, L, _THIS) -> [{K,[V]}|L].

collapse(L, THIS) ->
    AggrL = lists:foldl( fun aggregateValues/3, [], lists:keysort(1, L), THIS ),
    lists:keymap( fun lists:sort/1, 2, lists:reverse(AggrL)).


mkHdr ({Key,Values}, _THIS) ->
    Key ++ ":" ++ erlaws_util:mkEnumeration(Values,",").

canonicalizeAmzHeaders( Headers, THIS ) ->
    XAmzHeaders = [ {string:to_lower(Key),Value} || {Key,Value} <- Headers, 
						    isAmzHeader(Key, THIS) ],
    Strings = lists:map( 
		fun mkHdr/2, 
		collapse(XAmzHeaders, THIS), THIS),
    erlaws_util:mkEnumeration( [[String, "\n"] || String <- Strings], "").

canonicalizeResource ( "", "", _THIS ) -> "/";
canonicalizeResource ( Bucket, "", _THIS ) -> "/" ++ Bucket ++ "/";
canonicalizeResource ( "", Path, _THIS) -> "/" ++ Path;
canonicalizeResource ( Bucket, Path, _THIS ) -> "/" ++ Bucket ++ "/" ++ Path.

makeParam(X, _THIS) ->
    case X of
	{_, []} -> {};
	{prefix, Prefix} -> 
	    {"prefix", Prefix};
	{marker, Marker} -> 
	    {"marker", Marker};
	{max_keys, MaxKeys} when is_integer(MaxKeys) -> 
	    {"max-keys", integer_to_list(MaxKeys)};
	{delimiter, Delimiter} -> 
	    {"delimiter", Delimiter};
	_ -> {}
    end.


buildHost("", _THIS) ->
    ?AWS_S3_HOST;
buildHost(Bucket, _THIS) ->
    Bucket ++ "." ++ ?AWS_S3_HOST.

buildProtocol({?MODULE, [_AWS_KEY, _AWS_SEC_KEY, SECURE]}) ->
	case SECURE of 
		true -> "https://";
		_ -> "http://" end.

buildUrl("", "", [], THIS) ->
    buildProtocol(THIS) ++ ?AWS_S3_HOST ++ "/";
buildUrl("", Path, [], THIS) ->
    buildProtocol(THIS) ++ ?AWS_S3_HOST ++ Path;
buildUrl(Bucket,Path,QueryParams,THIS) -> 
    buildProtocol(THIS) ++ Bucket ++ "." ++ ?AWS_S3_HOST ++ "/" ++ Path ++ 
	erlaws_util:queryParams(QueryParams).

buildContentHeaders(Contents, _THIS) when is_integer(Contents) -> 
    [{"Content-Length", integer_to_list(Contents)}];
% Detect gzip header and put appropriate Content-Encoding. Questionable?..
buildContentHeaders(<<16#1f, 16#8b, _/binary>> = Contents, _THIS) -> 
    [{"Content-Length", integer_to_list(iolist_size(Contents))},
     {"Content-Encoding", "gzip"}];
buildContentHeaders(Contents, _THIS) -> 
    [{"Content-Length", integer_to_list(iolist_size(Contents))}].

buildMetadataHeaders(Metadata, _THIS) ->
    lists:foldl(fun({Key, Value}, Acc) ->
		[{string:to_lower("x-amz-meta-"++Key), Value} | Acc]
	end, [], Metadata).

buildContentMD5Header(ContentMD5, _THIS) ->
    case ContentMD5 of
	"" -> [];
	_ -> [{"Content-MD5", ContentMD5}]
    end.

stringToSign ( Verb, ContentMD5, ContentType, Date, Bucket, Path, 
	       OriginalHeaders, THIS ) ->
    Parts = [ Verb, ContentMD5, ContentType, Date, 
	      canonicalizeAmzHeaders(OriginalHeaders, THIS)],
    erlaws_util:mkEnumeration( Parts, "\n") ++ 
	canonicalizeResource(Bucket, Path, THIS).

sign (Key,Data,_THIS) ->
    binary_to_list( base64:encode( crypto:sha_mac(Key,Data) ) ).

genericRequest( Method, Bucket, Path, QueryParams, Metadata,
		HTTPHeaders, Body, THIS ) ->
    genericRequest( Method, Bucket, Path, QueryParams, Metadata,
		    HTTPHeaders, Body, ?NR_OF_RETRIES, THIS).

genericRequest( Method, Bucket, Path, QueryParams, Metadata, 
	HTTPHeaders, Body, NrOfRetries, {?MODULE, [AWS_KEY, AWS_SEC_KEY, _SECURE]}=THIS) ->
    Date = httpd_util:rfc1123_date(erlang:localtime()),
    MethodString = string:to_upper( atom_to_list(Method) ),
    Url = buildUrl(Bucket,Path,QueryParams,THIS),

    ContentMD5 = case iolist_size(Body) of
		     0 -> "";
		     _ -> binary_to_list(base64:encode(erlang:md5(Body)))
		 end,
    
    Headers =
        buildContentHeaders(Body, THIS) ++
	buildMetadataHeaders(Metadata, THIS) ++ 
	buildContentMD5Header(ContentMD5, THIS) ++
	HTTPHeaders,

    ContentType = case [Value || {"Content-Type", Value} <- HTTPHeaders] of
		[CT|_] -> CT;
		[] -> ""
	end,
    
    {AccessKey, SecretAccessKey } = {AWS_KEY, AWS_SEC_KEY},

    Signature = sign(SecretAccessKey,
		     stringToSign(MethodString, ContentMD5, ContentType, Date,
				  Bucket,
				  Path ++ erlaws_util:queryParams(QueryParams),
				  Headers, THIS), THIS),
    
    FinalHeaders = [ {"Authorization","AWS " ++ AccessKey ++ ":" ++ Signature },
		     {"Host", buildHost(Bucket, THIS) },
		     {"Date", Date },
		     {"Expect", "Continue"}
		     | Headers ],

    Request = case Method of
 		  get -> { Url, FinalHeaders };
		  head -> { Url, FinalHeaders };
 		  put -> { Url, FinalHeaders, ContentType, Body };
 		  post -> { Url, FinalHeaders, ContentType, Body };
 		  delete -> { Url, FinalHeaders }
 	      end,

    HttpOptions = [{autoredirect, true}],
    Options = [ {sync,true}, {headers_as_is,true}, {body_format, binary} ],

    Reply = httpc:request( Method, Request, HttpOptions, Options ),
    
    %%     {ok, {Status, ReplyHeaders, RBody}} = Reply,
    %%     io:format("Response:~n ~p~n~p~n~p~n", [Status, ReplyHeaders, 
    %% 					   binary_to_list(RBody)]),
    
    case Reply of
 	{ok, {{_HttpVersion, Code, _ReasonPhrase}, ResponseHeaders, 
	      ResponseBody }} when Code=:=200; Code=:=204 -> 
 	    {ok, ResponseHeaders, ResponseBody};

	{ok, {{_HttpVersion, Code, ReasonPhrase}, ResponseHeaders, 
	      _ResponseBody }} when Code=:=500, NrOfRetries == 0 ->
	    throw ({error, {"500", ReasonPhrase, 
		    proplists:get_value(?S3_REQ_ID_HEADER, ResponseHeaders)}});
	
	{ok, {{_HttpVersion, Code, _ReasonPhrase}, _ResponseHeaders, 
	      _ResponseBody }} when Code=:=500 ->
	    timer:sleep((?NR_OF_RETRIES-NrOfRetries)*500),
	    genericRequest(Method, Bucket, Path, QueryParams, 
			   Metadata, HTTPHeaders, Body, NrOfRetries-1, THIS);
	
 	{ok, {{_HttpVersion, HttpCode, ReasonPhrase}, ResponseHeaders, 
	      ResponseBody }} ->
	    throw (try mkErr(ResponseBody, ResponseHeaders, THIS) of
		      {error, Reason} -> {error, Reason}
		  catch
		      exit:_Error ->
			  {error, {integer_to_list(HttpCode), ReasonPhrase, 
			   proplists:get_value(?S3_REQ_ID_HEADER, ResponseHeaders)}}
		  end)
    end.

mkErr (Xml, Headers, _THIS) ->
    {XmlDoc, _Rest} = xmerl_scan:string( binary_to_list(Xml) ),
    [#xmlText{value=ErrorCode}|_] = 
	xmerl_xpath:string("/Error/Code/text()", XmlDoc),
    [#xmlText{value=ErrorMessage}|_] = 
	xmerl_xpath:string("/Error/Message/text()", XmlDoc),
    {error, {ErrorCode, ErrorMessage, 
	     proplists:get_value(?S3_REQ_ID_HEADER, Headers)}}.

extractObjectInfo (Node, _THIS) -> 
    [Key|_] = xmerl_xpath:string("./Key/text()", Node),
    [ETag|_] = xmerl_xpath:string("./ETag/text()", Node),
    [LastModified|_] = xmerl_xpath:string("./LastModified/text()", Node),
    [Size|_] = xmerl_xpath:string("./Size/text()", Node),
    #s3_object_info{key=Key#xmlText.value, lastmodified=LastModified#xmlText.value,
		 etag=ETag#xmlText.value, size=Size#xmlText.value}.

openAndGetFileSize(FileName, _THIS) ->
    case file:open(FileName, [read, binary]) of
        {ok, File} ->
            {ok, #file_info{size=Size}} = file:read_file_info(FileName),
            {Size, File};
        _ ->
            {error, no_file}
    end.

sendData(Socket, File, _THIS) ->
    case file:read(File, ?CHUNK_SIZE) of
        {ok, Data} ->
            gen_tcp:send(Socket, Data),
            sendData(Socket, File, _THIS);
        eof ->
            ok
    end.
