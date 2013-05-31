%%-------------------------------------------------------------------
%% @author Sascha Matzke <sascha.matzke@didolo.org>
%% @copyright 2007 Sascha Matzke
%% @doc This is an client implementation for Amazon's SimpleDB WebService
%% (This is a forked version by Kazuhiro Ogura <rgoura@karesansui-project.info>)
%% @end
%%%-------------------------------------------------------------------

-module(erlaws_sdb).

%% exports
-export([new/3]).
-export([create_domain/2, delete_domain/2, list_domains/1, list_domains/2,
	 put_attributes/4, put_attributes/5, batch_put_attributes/3,
	 delete_item/3, delete_attributes/4, delete_attributes/5,
	 get_attributes/3, get_attributes/4, get_attributes/5,
	 list_items/2, list_items/3, 
	 query_items/3, query_items/4, select/2, select/3, storage_size/3]).

%% include record definitions
-include_lib("xmerl/include/xmerl.hrl").

-define(AWS_SDB_HOST, "sdb.amazonaws.com").
-define(OLD_AWS_SDB_VERSION, "2007-11-07").
-define(AWS_SDB_VERSION, "2009-04-15").
-define(USE_SIGNATURE_V1, false).

new(AWS_KEY, AWS_SEC_KEY, SECURE) ->
	{?MODULE, [AWS_KEY, AWS_SEC_KEY, SECURE]}.

%% This function creates a new SimpleDB domain. The domain name must be unique among the 
%% domains associated with your AWS Access Key ID. This function might take 10 
%% or more seconds to complete.. 
%%
%% Spec: create_domain(Domain::string()) -> 
%%       {ok, Domain::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}} 
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NumberDomainsExceeded"
%%
create_domain(Domain, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest("CreateDomain", 
		       Domain, "", [], [], THIS) of
	{ok, Body} ->
		{XmlDoc, _Rest} = xmerl_scan:string(Body),
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc), 
	    {ok, {requestId, RequestId}}
    catch 
	throw:{error, Descr} -> 
	    {error, Descr}
    end.

%% This function deletes a SimpleDB domain. Any items (and their attributes) in the domain 
%% are deleted as well. This function might take 10 or more seconds to complete.
%% 
%% Spec: delete_domain(Domain::string()) -> 
%%       {ok, {requestId, RequestId:string()}} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%       
%%       Code::string() -> "MissingParameter"
%%
delete_domain(Domain, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest("DeleteDomain", 
		       Domain, "", [], [], THIS) of
	{ok, Body} -> 
		{XmlDoc, _Rest} = xmerl_scan:string(Body),
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc), 
    	{ok, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Lists all domains associated with your Access Key ID. 
%% 
%% Spec: list_domains() -> 
%%       {ok, DomainNames::[string()], ""} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}} 
%%
%% See list_domains/1 for a detailed error description
%%
list_domains({?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    list_domains([], THIS).

%% Lists domains up to the limit set by {max_domains, integer()}.
%% A NextToken is returned if there are more than max_domains domains. 
%% Calling list_domains successive times with the NextToken returns up
%% to max_domains more domain names each time.
%%
%% Spec: list_domains(Options::[{atom, (string() | integer())}]) ->
%%       {ok, DomainNames::[string()], [], {requestId, ReqId::string()}} |
%%       {ok, DomainNames::[string()], NextToken::string(), {requestId, ReqId::string()}} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Options -> [{max_domains, integer()}, {next_token, string()}]
%%
%%       Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | "MissingParameter"
%%
list_domains(Options, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    try genericRequest("ListDomains", "", "", [], 
				[makeParam(X, THIS) || X <- Options], THIS) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    DomainNodes = xmerl_xpath:string("//ListDomainsResult/DomainName/text()", 
					     XmlDoc),
	    NextToken = case xmerl_xpath:string("//ListDomainsResult/NextToken/text()", 
						XmlDoc) of
			    [] -> "";
			    [#xmlText{value=NT}|_] -> NT
			end,
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    {ok, [Node#xmlText.value || Node <- DomainNodes], NextToken, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.
    
%% This function creates or replaces attributes in an item. You specify new 
%% attributes using a list of tuples. Attributes are uniquely identified in 
%% an item by their name/value combination. For example, a single item can 
%% have the attributes { "first_name", "first_value" } and { "first_name", 
%% second_value" }. However, it cannot have two attribute instances where 
%% both the attribute name and value are the same.
%%
%% Optionally, you can supply the Replace parameter for each individual 
%% attribute. Setting this value to true causes the new attribute value 
%% to replace the existing attribute value(s). For example, if an item has 
%% the attributes { "a", ["1"] }, { "b", ["2","3"]} and you call this function 
%% using the attributes { "b", "4", true }, the final attributes of the item 
%% are changed to { "a", ["1"] } and { "b", ["4"] }, which replaces the previous 
%% values of the "b" attribute with the new value.
%%
%% Using this function to replace attribute values that do not exist will not 
%% result in an error.
%%
%% The following limitations are enforced for this operation:
%% - 100 attributes per each call
%% - 256 total attribute name-value pairs per item
%% - 250 million attributes per domain
%% - 10 GB of total user data storage per domain
%% - 1 billion attributes per domain
%%
%% Spec: put_attributes(Domain::string(), Item::string(), 
%%                      Attributes::[{Name::string(), (Value::string() | Values:[string()])}]) |
%%       put_attributes(Domain::string(), Item::string(), 
%%                      Attributes::[{Name::string(), (Value::string() | Values:[string()]), 
%%                                    Replace -> true}]) ->
%%       {ok} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain" |
%%                         "NumberItemAttributesExceeded" | "NumberDomainAttributesExceeded" |
%%                         "NumberDomainBytesExceeded"
%%
put_attributes(Domain, Item, Attributes, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS)
					when is_list(Domain),
						is_list(Item),
						is_list(Attributes) ->
    put_attributes(Domain, Item, Attributes, [], THIS).

%% You may request "conditional put" by providing the 4th argument to
%% put_attributes/4.
%% For example, if you expect AttributeName to be AttribueValue,
%% provide [{expected, AttributeName, AttributeValue}], and if 
%% you expect [AttributeName] not to exist, provide
%% [{expected, AttributeName, false}].
%%
%% See: http://developer.amazonwebservices.com/connect/entry.jspa?externalID=3572
put_attributes(Domain, Item, Attributes, Options, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Domain),
					      is_list(Item),
					      is_list(Attributes),
					      is_list(Options) ->
    try genericRequest("PutAttributes", Domain, Item, 
		   Attributes, [makeParam(X, THIS) || X <- Options], THIS) of
	{ok, Body} -> 
		{XmlDoc, _Rest} = xmerl_scan:string(Body),
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
		{ok, {requestId, RequestId}}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Deletes one or more attributes associated with the item. 
%% 
%% Spec: delete_attributes(Domain::string(), Item::string, Attributes::[string()]) ->
%%       {ok, {requestId, ReqId::string()}} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain"
%%
delete_attributes(Domain, Item, Attributes, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Domain),
						 is_list(Item),
						 is_list(Attributes) ->
    delete_attributes(Domain, Item, Attributes, [], THIS).

%% You may request "conditional delete" by providing the 4th argument to
%% put_attributes/4.
%% For example, if you expect AttributeName to be AttribueValue,
%% provide [{expected, AttributeName, AttributeValue}], and if 
%% you expect [AttributeName] not to exist, provide
%% [{expected, AttributeName, false}].
%%
%% See: http://developer.amazonwebservices.com/connect/entry.jspa?externalID=3572
delete_attributes(Domain, Item, Attributes, Options, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Domain),
							  is_list(Item),
							  is_list(Attributes),
							  is_list(Options) ->
    try genericRequest("DeleteAttributes", Domain, Item,
		       Attributes, [makeParam(X, THIS) || X <- Options], THIS) of
	{ok, Body} -> 
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    [#xmlText{value=RequestId}|_] =
		xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    {ok, {requestId, RequestId}}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Creates or replace multiple item attributes at once.
%% You specify attributes using a list of tuples, each tuple formed as
%% {"Item1", [{"Attr1", ["Value1"]}, {"Attr2", ["Value2", "Value3"]}]}.
%%
%% You may also use the Replace parameter as put_attributes, as
%% {"Item1", [{"Attr1", "Value1"}, {"Attr2", ["Value2", "Value3", replace}]}.
%%
%% For API details, see:
%% http://docs.amazonwebservices.com/AmazonSimpleDB/2009-04-15/DeveloperGuide/index.html?SDB_API_BatchPutAttributes.html
%%
%% Additional to the put_attributes limitation,
%% following limitation are enforced.
%% - 25 item limit per BatchPutAttribution operation
%% - 1 MB request size
%%
%% Spec: batch_put_attributes(Domain::string(), 
%%                            ItemAttributes::[{Item::string(),
%%                                              Attributes::[{Name::string(), (Value::string() | Values:[string()])}])}] |
%% Spec: batch_put_attributes(Domain::string(), 
%%                            ItemAttributes::[{Name::string(), (Value::string() | Values:[string()]), 
%%                                             Replace -> true}])}] ->
%%
%%       {ok, {requestId, RequestId::string()}} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "DuplicateItemName" | "InvalidParameterValue" | "MissingParameter" |
%%                         "NoSuchDomain" | "NumberItemAttributesExceeded" |
%%                         "NumberDomainAttributesExceeded" | "NumberDomainBytesExceeded" |
%%                         "NumberSubmittedItemsExceeded" | "NumberSubmittedAttrubutesExceeded"
%%
batch_put_attributes(Domain, ItemAttributes, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Domain),
						  is_list(ItemAttributes) ->
    try genericRequest("BatchPutAttributes", Domain, "", 
		   ItemAttributes, [], THIS) of
	{ok, Body} -> 
		{XmlDoc, _Rest} = xmerl_scan:string(Body),
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
		{ok, {requestId, RequestId}}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Deletes the specified item. 
%% 
%% Spec: delete_item(Domain::string(), Item::string()) ->
%%       {ok} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain"
%%
delete_item(Domain, Item, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Domain), 
			       is_list(Item) ->
    try delete_attributes(Domain, Item, [], THIS) of
		{ok, RequestId} -> {ok, RequestId}
    catch
		throw:{error, Descr} ->
	    	{error, Descr}
    end.

%% Returns all of the attributes associated with the items in the given list.
%%
%% If the item does not exist on the replica that was accessed for this 
%% operation, an empty set is returned. The system does not return an 
%% error as it cannot guarantee the item does not exist on other replicas.
%%
%% Note: Currently SimpleDB is only capable of returning the attributes for
%%       a single item. To work around this limitation, this function starts
%%       length(Items) parallel requests to sdb and aggregates the results.
%%
%% Spec: get_attributes(Domain::string(), [Item::string(),..]) -> 
%%       {ok, Items::[{Item, Attributes::[{Name::string(), Values::[string()]}]}]} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain"
%%
get_attributes(Domain, Items, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Domain),
				   is_list(Items), 
				   is_list(hd(Items)) ->
    get_attributes(Domain, Items, "", THIS);

%% Returns all of the attributes associated with the item.
%%
%% If the item does not exist on the replica that was accessed for this 
%% operation, an empty set is returned. The system does not return an 
%% error as it cannot guarantee the item does not exist on other replicas.
%%
%% Note: Currently SimpleDB is only capable of returning the attributes for
%%       a single item. To be compatible with a possible future this function
%%       returns a list of {Item, Attributes::[{Name::string(), Values::[string()]}]}
%%       tuples. For the time being this list has exactly one member.
%%
%% Spec: get_attributes(Domain::string(), Item::string()) -> 
%%       {ok, Items::[{Item, Attributes::[{Name::string(), Values::[string()]}]}]} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain"
%%
get_attributes(Domain, Item, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Domain),
				  is_list(Item) ->
    get_attributes(Domain, Item, "", THIS).

%% Returns the requested attribute for a list of items. 
%%
%% See get_attributes/2 for further documentation.
%%
%% Spec: get_attributes(Domain::string(), [Item::string(),...], Attribute::string()) -> 
%%       {ok, Items::[{Item, Attribute::[{Name::string(), Values::[string()]}]}]} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain"
%%
get_attributes(Domain, Items, Attribute, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Domain),
					      is_list(Items), 
					      is_list(Attribute) ->
    get_attributes(Domain, Items, Attribute, [], THIS).

get_attributes(Domain, Items, Attribute, Options, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Domain),
						       is_list(Items), 
						       is_list(hd(Items)),
						       is_list(Attribute),
						       is_list(Options) ->
    Fetch = fun(X) -> 
		    ParentPID = self(), 
		    spawn(fun() ->
				  case get_attributes(Domain, X, Attribute, Options, THIS) of
				      {ok, [ItemResult]} ->
					  ParentPID ! { ok, ItemResult };
				      {error, Descr} -> 
					  ParentPID ! {error, Descr}
				  end
			  end)
	    end,
    Receive= fun(_) ->
		     receive 
			 { ok, Anything } -> Anything;
			 { error, Descr } -> {error, Descr }
		     end
	     end,
    lists:foreach(Fetch, Items),
    Results = lists:map(Receive, Items),
    case proplists:get_all_values(error, Results) of 
	[] -> {ok, Results};
	[Error|_Rest] -> {error, Error}
    end;

%% Returns the requested attribute for an item. 
%%
%% See get_attributes/2 for further documentation.
%%
%% Spec: get_attributes(Domain::string(), Item::string(), Attribute::string()) -> 
%%       {ok, Items::[{Item, Attribute::[{Name::string(), Values::[string()]}]}]} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "MissingParameter" | "NoSuchDomain"
%%
get_attributes(Domain, Item, Attribute, Options, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Domain),
						      is_list(Item),
						      is_list(Attribute),
						      is_list(Options) ->
    try genericRequest("GetAttributes", Domain, Item,
		       Attribute, [makeParam(X, THIS) || X <- Options], THIS) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    AttrList = [{KN, VN} || Node <- xmerl_xpath:string("//Attribute", XmlDoc),
				    begin
					[#xmlText{value=KeyRaw}|_] = 
					    xmerl_xpath:string("./Name/text()", Node),
					KN = case xmerl_xpath:string("./Name/@encoding", Node) of 
						[#xmlAttribute{value="base64"}|_] -> base64:decode(KeyRaw);
						_ -> KeyRaw end,
					ValueRaw = 
					    lists:flatten([ ValueR || #xmlText{value=ValueR} <- xmerl_xpath:string("./Value/text()", Node)]),
					VN = case xmerl_xpath:string("./Value/@encoding", Node) of 
						[#xmlAttribute{value="base64"}|_] -> base64:decode(ValueRaw);
						_ -> ValueRaw end,
					true
				    end],
	    {ok, [{Item, lists:foldr(fun aggregateAttr/3, [], AttrList, THIS)}]}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.


%% Returns a list of all items of a domain - 100 at a time. If your
%% domains contains more then 100 item you must use list_items/2 to
%% retrieve all items.
%%
%% WARNING: This function depends on deprecated operation "Query"
%% which exists only on old SimpleDB API (version 2007-11-07).
%%
%% Spec: list_items(Domain::string()) ->
%%       {ok, Items::[string()], []} |
%%       {ok, Items::[string()], NextToken::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | 
%%                         "MissingParameter" | "NoSuchDomain"
%%  
list_items(Domain, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    list_items(Domain, [], THIS).


%% Returns up to max_items -> integer() <= 250 items of a domain. If
%% the total item count exceeds max_items you must call this function
%% again with the NextToken value provided in the return value.
%%
%% WARNING: This function depends on deprecated operation "Query"
%% which exists only on old SimpleDB API (version 2007-11-07).
%%
%% Spec: list_items(Domain::string(), Options::[{atom(), (integer() | string())}]) ->
%%       {ok, Items::[string()], []} |
%%       {ok, Items::[string()], NextToken::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Options -> [{max_items, integer()}, {next_token, string()}]
%% 
%%       Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | 
%%                         "MissingParameter" | "NoSuchDomain"
%%  
list_items(Domain, Options, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Options) ->
    try genericRequest("Query", Domain, "", [], 
		       [makeParam(X, THIS) || X <- [{version, ?OLD_AWS_SDB_VERSION}|Options]], THIS) of
	{ok, Body} ->
	    {XmlDoc, _Rest} = xmerl_scan:string(Body),
	    ItemNodes = xmerl_xpath:string("//ItemName/text()", XmlDoc),
	    NextToken = case xmerl_xpath:string("//NextToken/text()", XmlDoc) of
			    [] -> "";
			    [#xmlText{value=NT}|_] -> NT
			end,
		[#xmlText{value=RequestId}|_] =
			xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
	    {ok, [Node#xmlText.value || Node <- ItemNodes], NextToken, {requestId, RequestId}}
    catch
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Executes the given select expression.
%% http://docs.amazonwebservices.com/AmazonSimpleDB/2009-04-15/DeveloperGuide/index.html?SDB_API_Select.html
%%
%% Spec: select(SelectExp::string(),
%%              Options::[{atom(), (integer() | string())}]) ->
%%     {ok, Items::[{Item, Attribute::[{Name::string(), Values::[string()]}]}], []} |
%%     {ok, Items::[{Item, Attribute::[{Name::string(), Values::[string()]}]}], NextToken:string()}
%%
%%     {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%     Options -> [{next_token, string()}]
%%
%%     Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | "InvalidNumberPredicates"
%%                     | "InvalidNumberValueTests" | "InvalidQueryExpression" | "InvalidSortExpression"
%%                     | "MissingParameter" | "NoSuchDomain" | "RequestTimeout" | "TooManyRequestAttributes"
select(SelectExp, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    select(SelectExp, [], THIS).

select(SelectExp, Options, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Options) ->
    try genericRequest("Select", "", "", [],
		       [{"SelectExpression", SelectExp}|
			[makeParam(X, THIS) || X <- Options]], THIS) of
	{ok, Body} ->
	    {XmlDoc_, _Rest} = xmerl_scan:string(Body),
	    ItemNodes = xmerl_xpath:string("//Item", XmlDoc_),
	    NextToken = case xmerl_xpath:string("//NextToken/text()", XmlDoc_) of
			    [] -> "";
			    [#xmlText{value=NT}|_] -> NT
			end,
	    F = fun(XmlDoc) ->
			[#xmlText{value=Item}|_] = xmerl_xpath:string("//Name/text()", XmlDoc),
			AttrList = [{KN, VN} || Node <- xmerl_xpath:string("//Attribute", XmlDoc),
						begin
						    [#xmlText{value=KeyRaw}|_] = 
							xmerl_xpath:string("./Name/text()", Node),
						    KN = case xmerl_xpath:string("./Name/@encoding", Node) of 
							     [#xmlAttribute{value="base64"}|_] -> base64:decode(KeyRaw);
							     _ -> KeyRaw end,
						    ValueRaw = 
							lists:flatten([ ValueR || #xmlText{value=ValueR} <- xmerl_xpath:string("./Value/text()", Node)]),
						    VN = case xmerl_xpath:string("./Value/@encoding", Node) of 
							     [#xmlAttribute{value="base64"}|_] -> base64:decode(ValueRaw);
							     _ -> ValueRaw end,
						    true
						end],
			{Item, lists:foldr(fun aggregateAttr/3, [], AttrList, THIS)}
		end,
	    Items = [F(ItemNode) || ItemNode <- ItemNodes],
	    {ok, Items, NextToken}
    catch 
	throw:{error, Descr} ->
	    {error, Descr}
    end.

%% Executes the given query expression against a domain. The syntax for
%% such a query spec is documented here:
%% http://docs.amazonwebservices.com/AmazonSimpleDB/2007-11-07/DeveloperGuide/SDB_API_Query.html
%%
%% DEPRECATION WARNING: This function depends on deprecated operation "Query"
%% which exists only on old SimpleDB API (version 2007-11-07).
%%
%% Spec: query_items(Domain::string(), QueryExp::string()]) ->
%%       {ok, Items::[string()], []} |
%%       {ok, Items::[string()], NextToken::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | 
%%                         "MissingParameter" | "NoSuchDomain"
%%  
query_items(Domain, QueryExp, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    query_items(Domain, QueryExp, [], THIS).

%% Executes the given query expression against a domain. The syntax for
%% such a query spec is documented here:
%% http://docs.amazonwebservices.com/AmazonSimpleDB/2007-11-07/DeveloperGuide/SDB_API_Query.html
%%
%% DEPRECATION WARNING: This function depends on deprecated operation "Query"
%% which exists only on old SimpleDB API (version 2007-11-07).
%%
%% Spec: list_items(Domain::string(), QueryExp::string(), 
%%                  Options::[{atom(), (integer() | string())}]) ->
%%       {ok, Items::[string()], []} |
%%       {ok, Items::[string()], NextToken::string()} |
%%       {error, {Code::string(), Msg::string(), ReqId::string()}}
%%
%%       Options -> [{max_items, integer()}, {next_token, string()}]
%% 
%%       Code::string() -> "InvalidParameterValue" | "InvalidNextToken" | 
%%                         "MissingParameter" | "NoSuchDomain"
%%  
query_items(Domain, QueryExp, Options, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when is_list(Options) ->
    {ok, Body} = genericRequest("Query", Domain, "", [], 
				[{"QueryExpression", QueryExp}|
				 [makeParam(X, THIS) || X <- [{version, ?OLD_AWS_SDB_VERSION}|Options]]], THIS),
    {XmlDoc, _Rest} = xmerl_scan:string(Body),
    ItemNodes = xmerl_xpath:string("//ItemName/text()", XmlDoc),
	[#xmlText{value=RequestId}|_] =
		xmerl_xpath:string("//ResponseMetadata/RequestId/text()", XmlDoc),
    {ok, [Node#xmlText.value || Node <- ItemNodes], {requestId, RequestId}}.


%% storage cost

storage_size(Item, Attributes, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    ItemSize = length(Item) + 45,
    {AttrSize, ValueSize} = calcAttrStorageSize(Attributes, THIS),
    {AttrSize, ItemSize + ValueSize}.

%% internal functions

calcAttrStorageSize(Attributes, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    calcAttrStorageSize(Attributes, {0, 0}, THIS).

calcAttrStorageSize([{Attr, ValueList}|Rest], {AttrSize, ValueSize}, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    calcAttrStorageSize(Rest, {AttrSize + length(Attr) + 45, 
			       calcValueStorageSize(ValueSize, ValueList, THIS)}, THIS);
calcAttrStorageSize([], Result, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}) ->
    Result.

calcValueStorageSize(ValueSize, [Value|Rest], {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    calcValueStorageSize(ValueSize + length(Value) + 45, Rest, THIS);
calcValueStorageSize(ValueSize, [], {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}) ->
    ValueSize.

sign (Key,Data, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}) ->
    %io:format("StringToSign:~n ~p~n", [Data]),
    binary_to_list( base64:encode( crypto:sha_mac(Key,Data) ) ).

genericRequest(Action, Domain, Item,
	Attributes, Options, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) when ?USE_SIGNATURE_V1 ->
    genericRequestV1(Action, Domain, Item, Attributes, Options, THIS);
genericRequest(Action, Domain, Item, 
	Attributes, Options, {?MODULE, [AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    io:format("Options: ~p~n", [Options]),
    Timestamp = lists:flatten(erlaws_util:get_timestamp()),
    ActionQueryParams = getQueryParams(Action, Domain, Item, Attributes, 
				       lists:flatten(Options), THIS),
    Params =  [{"AWSAccessKeyId", AWS_KEY},
			{"Action", Action}, 
			{"Timestamp", Timestamp}
	      ] ++ case lists:keyfind("Version", 1, ActionQueryParams) of
		       false ->
			   [{"Version", ?AWS_SDB_VERSION}| ActionQueryParams];
		       _ ->
			   ActionQueryParams
		   end,
    Result = mkReq(Params, THIS),
    case Result of
	{ok, _Status, Body} ->
	    {ok, Body};
	{error, {_Proto, _Code, _Reason}, Body} ->
	    %throw({error, {integer_to_list(Code), Reason}, mkErr(Body)})
	    throw({error, mkErr(Body, THIS)})
    end.


getQueryParams("CreateDomain", Domain, _Item, _Attributes, _Options, _THIS) ->
    [{"DomainName", Domain}];
getQueryParams("DeleteDomain", Domain, _Item, _Attributes, _Options, _THIS) ->
    [{"DomainName", Domain}];
getQueryParams("ListDomains", _Domain, _Item, _Attributes, Options, _THIS) ->
    Options;
getQueryParams("PutAttributes", Domain, Item, Attributes, Options, THIS) ->
    Options ++ [{"DomainName", Domain}, {"ItemName", Item}] ++
	buildAttributeParams(Attributes, THIS);
getQueryParams("BatchPutAttributes", Domain, _Item, ItemAttributes, _Options, THIS) ->
    [{"DomainName", Domain}| 
	buildBatchAttributeParams(ItemAttributes, THIS)];
getQueryParams("GetAttributes", Domain, Item, Attribute, Options, _THIS) ->
    Options ++ [{"DomainName", Domain}, {"ItemName", Item}] ++
	if length(Attribute) > 0 ->
		[{"AttributeName", Attribute}];
	   true -> []
	end;
getQueryParams("DeleteAttributes", Domain, Item, Attributes, Options, THIS) ->
   Options ++ [{"DomainName", Domain}, {"ItemName", Item}] ++
	if length(Attributes) > 0 -> 
		buildAttributeParams(Attributes, THIS);
	   true -> []
	end;
getQueryParams("Select", _Domain, _Item, _Attributes, Options, _THIS) ->
    Options;
getQueryParams("Query", Domain, _Item, _Attributes, Options, _THIS) ->
    [{"DomainName", Domain}] ++ Options.

getProtocol({?MODULE, [_AWS_KEY, _AWS_SEC_KEY, SECURE]}) ->
	case SECURE of 
		true -> "https://";
		_ -> "http://" end.

mkReq(Params, {?MODULE, [_AWS_KEY, AWS_SEC_KEY, _SECURE]}=THIS) ->
    QueryParams = [{"SignatureVersion", "2"}|[{"SignatureMethod", "HmacSHA1"}|Params]],
    io:format("~p~n", [QueryParams]),
    ParamsString = erlaws_util:mkEnumeration([ erlaws_util:url_encode(Key) ++ "=" ++ erlaws_util:url_encode(Value) ||
						 {Key, Value} <- lists:keysort(1, QueryParams)],
					     "&"),
    StringToSign = "POST\n" ++ string:to_lower(?AWS_SDB_HOST) ++ "\n" ++ "/" ++ "\n" ++ ParamsString,
    Signature = sign(AWS_SEC_KEY, StringToSign, THIS),
    SignatureString = "&Signature=" ++ erlaws_util:url_encode(Signature),
    Url = getProtocol(THIS) ++ ?AWS_SDB_HOST ++ "/",
    PostData = ParamsString ++ SignatureString,
    %io:format("~s~n~s~n", [Url, PostData]),
    Request = {Url, [], "application/x-www-form-urlencoded", PostData},
    HttpOptions = [{autoredirect, true}],
    Options = [{sync,true}, {body_format, binary}],
    {ok, {Status, _ReplyHeaders, Body}} =
	httpc:request(post, Request, HttpOptions, Options),
    case Status of 
	{_, 200, _} -> {ok, Status, binary_to_list(Body)};
	{_, _, _} -> {error, Status, binary_to_list(Body)}
    end.

buildAttributeParams(Attributes, THIS) ->
    CAttr = collapse(Attributes, THIS),
    {_C, L} = lists:foldl(fun flattenParams/3, {0, []}, CAttr, THIS),
    %io:format("FlattenedList:~n ~p~n", [L]),
    lists:reverse(L).

buildBatchAttributeParams(ItemAttributes, THIS) ->
    BuildItemHdrFun =
	fun(F, IAs, ItemNum, Acc) ->
		ItemNumL = integer_to_list(ItemNum),
		Lead = "Item." ++ ItemNumL ++ ".",
		case IAs of
		    [] -> Acc;
		    [{Item, Attributes}|T] ->
			F(F, T, ItemNum + 1,
			  lists:foldl(fun({X, Y}, AccIn) ->
					      [{Lead ++ X, Y}|AccIn]
				      end,
				      [],
				      buildAttributeParams(Attributes, THIS))
			  ++ [{Lead ++ "ItemName", Item}|Acc])
		end
	end,
    lists:reverse(BuildItemHdrFun(BuildItemHdrFun, ItemAttributes, 0, [])).

mkEntryName(Counter, Key, _THIS) ->
    {"Attribute." ++ integer_to_list(Counter) ++ ".Name", Key}.
mkEntryValue(Counter, Value, _THIS) ->
    {"Attribute."++integer_to_list(Counter) ++ ".Value", Value}.

flattenParams({K, V, R}, {C, L}, THIS) ->
    PreResult = if R ->
			{C, [{"Attribute." ++ integer_to_list(C) 
			      ++ ".Replace", "true"} | L]};
		   true -> {C, L}
		end,
    FlattenVal = fun(Val, {Counter, ResultList}) ->
			 %io:format("~p -> ~p ~n", [K, Val]),
			 NextCounter = Counter + 1,
			 EntryName = mkEntryName(Counter, K, THIS),
			 EntryValue = mkEntryValue(Counter, Val, THIS),
			 {NextCounter,  [EntryValue | [EntryName | ResultList]]}
		 end,
    if length(V) > 0 ->
	    lists:foldl(FlattenVal, PreResult, V);
       length(V) =:= 0 -> {C + 1, [mkEntryName(C, K, THIS) | L]}
    end.

aggrV({K,V,true}, [{K,L,_OR}|T], _THIS) when is_list(V),
				      is_list(hd(V)) -> 
    [{K,V ++ L, true}|T];
aggrV({K,V,true}, [{K,L,_OR}|T], _THIS) -> [{K,[V|L], true}|T];

aggrV({K,V,false}, [{K, L, OR}|T], _THIS) when is_list(V),
					is_list(hd(V)) -> 
    [{K, V ++ L, OR}|T];
aggrV({K,V,false}, [{K, L, OR}|T], _THIS) -> [{K, [V|L], OR}|T];

aggrV({K,V}, [{K,L,OR}|T], _THIS) when is_list(V),
				is_list(hd(V))-> 
    [{K,V ++ L,OR}|T];
aggrV({K,V}, [{K,L,OR}|T], _THIS) -> [{K,[V|L],OR}|T];

aggrV({K,V,R}, L, _THIS) when is_list(V),
		       is_list(hd(V)) -> [{K, V, R}|L];
aggrV({K,V,R}, L, _THIS) -> [{K,[V], R}|L];

aggrV({K,V}, L, _THIS) when is_list(V),
		     is_list(hd(V)) -> [{K,V,false}|L];
aggrV({K,V}, L, _THIS) -> [{K,[V],false}|L];

aggrV(K, L, _THIS) -> [{K, [], false}|L].

collapse(L, THIS) ->
    AggrL = lists:foldl( fun aggrV/3, [], lists:keysort(1, L), THIS),
    lists:keymap( fun lists:sort/1, 2, lists:reverse(AggrL)).

makeParam(X, _THIS) ->
    case X of
	{_, []} -> {};
	{max_items, MaxItems} when is_integer(MaxItems) ->
	    {"MaxNumberOfItems", integer_to_list(MaxItems)};
	{max_domains, MaxDomains} when is_integer(MaxDomains) ->
	    {"MaxNumberOfDomains", integer_to_list(MaxDomains)};
	{next_token, NextToken} ->
	    {"NextToken", NextToken};
	{consistent_read, ConsistentRead} when is_boolean(ConsistentRead) ->
	    {"ConsistentRead", case ConsistentRead of true -> "true"; false -> "false" end};
	{expected, Name, Exists} when is_list(Name), is_boolean(Exists) ->
	    [{"Expected.1.Name", Name},
	     {"Expected.1.Exists", case Exists of true -> "true"; false -> "false" end}];
	{expected, Name, Value} when is_list(Name), is_list(Value) ->
	    [{"Expected.1.Name", Name},
	     {"Expected.1.Value", Value}];
	{version, Version} when is_list(Version) ->
	    {"Version", Version};
	_ -> {}
    end.


aggregateAttr ({K,V}, [{K,L}|T], _THIS) -> [{K,[V|L]}|T];
aggregateAttr ({K,V}, L, _THIS) -> [{K,[V]}|L].

mkErr(Xml, _THIS) ->
    {XmlDoc, _Rest} = xmerl_scan:string( Xml ),
    [#xmlText{value=ErrorCode}|_] = xmerl_xpath:string("//Error/Code/text()", XmlDoc),
    [#xmlText{value=ErrorMessage}|_] = xmerl_xpath:string("//Error/Message/text()", XmlDoc),
    [#xmlText{value=RequestId}|_] = xmerl_xpath:string("//RequestID/text()", XmlDoc),
    {ErrorCode, ErrorMessage, RequestId}.


%%% Erlang/OTP Releases before R14A can't handle Signature ver.2 + SSL request
%%% implemented above (plain requests work fine). Signature ver.1 based code are
%%% left below for compatibility.

genericRequestV1(Action, Domain, Item, 
	Attributes, Options, {?MODULE, [AWS_KEY, AWS_SEC_KEY, _SECURE]}=THIS) ->
    Timestamp = lists:flatten(erlaws_util:get_timestamp()),
    ActionQueryParams = getQueryParams(Action, Domain, Item, Attributes, 
				       lists:flatten(Options), THIS),
	SignParams = [{"AWSAccessKeyId", AWS_KEY},
			{"Action", Action}, 
			{"SignatureVersion", "1"},
			{"Timestamp", Timestamp}
		     ] ++ case lists:keyfind("Version", 1, ActionQueryParams) of
			      false ->
				  [{"Version", ?AWS_SDB_VERSION}| ActionQueryParams];
			      _ ->
				  ActionQueryParams
			  end,
	StringToSign = erlaws_util:mkEnumeration([Param++Value || {Param, Value} <- lists:sort(fun (A, B) -> 
		{KeyA, _} = A,
		{KeyB, _} = B,
		string:to_lower(KeyA) =< string:to_lower(KeyB) end, 
		SignParams)], ""),		

    Signature = sign(AWS_SEC_KEY, StringToSign, THIS),
    FinalQueryParams = SignParams ++ [{"Signature", Signature}],
    Result = mkReqV1(FinalQueryParams, THIS),
    case Result of
	{ok, _Status, Body} ->
	    {ok, Body};
	{error, {_Proto, _Code, _Reason}, Body} ->
	    %throw({error, {integer_to_list(Code), Reason}, mkErr(Body)})
	    throw({error, mkErr(Body, THIS)})
    end.

mkReqV1(QueryParams, {?MODULE, [_AWS_KEY, _AWS_SEC_KEY, _SECURE]}=THIS) ->
    %io:format("QueryParams:~n ~p~n", [QueryParams]),
    Url = getProtocol(THIS) ++ ?AWS_SDB_HOST ++ "/" ++ erlaws_util:queryParams( QueryParams ),
    %io:format("RequestUrl:~n ~p~n", [Url]),
    Request = {Url, []},
    HttpOptions = [{autoredirect, true}],
    Options = [ {sync,true}, {headers_as_is,true}, {body_format, binary} ],
    {ok, {Status, _ReplyHeaders, Body}} = 
	http:request(get, Request, HttpOptions, Options),
    %io:format("Response:~n ~p~n", [binary_to_list(Body)]),
    case Status of 
	{_, 200, _} -> {ok, Status, binary_to_list(Body)};
	{_, _, _} -> {error, Status, binary_to_list(Body)}
    end.
