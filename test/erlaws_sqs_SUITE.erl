%% common_test suite for erlaws_sqs

-module(erlaws_sqs_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("../include/erlaws.hrl").

-compile(export_all).

%%--------------------------------------------------------------------
%% Function: suite() -> Info
%%
%% Info = [tuple()]
%%   List of key/value pairs.
%%
%% Description: Returns list of tuples to set default properties
%%              for the suite.
%%
%% Note: The suite/0 function is only meant to be used to return
%% default data values, not perform any other operations.
%%--------------------------------------------------------------------
suite() -> [{timetrap, {seconds, 20}}].

%%--------------------------------------------------------------------
%% Function: groups() -> [Group]
%%
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%%   The name of the group.
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%%   Group properties that may be combined.
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%%   The name of a test case.
%% Shuffle = shuffle | {shuffle,Seed}
%%   To get cases executed in random order.
%% Seed = {integer(),integer(),integer()}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%%   To get execution of cases repeated.
%% N = integer() | forever
%%
%% Description: Returns a list of test case group definitions.
%%--------------------------------------------------------------------
groups() -> [].

%%--------------------------------------------------------------------
%% Function: all() -> GroupsAndTestCases
%%
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%%   Name of a test case group.
%% TestCase = atom()
%%   Name of a test case.
%%
%% Description: Returns the list of groups and test cases that
%%              are to be executed.
%%
%%      NB: By default, we export all 1-arity user defined functions
%%--------------------------------------------------------------------
all() ->
    [ {exports, Functions} | _ ] = ?MODULE:module_info(),
    [ FName || {FName, _} <- lists:filter(
                               fun ({module_info,_}) -> false;
                                   ({all,_}) -> false;
                                   ({init_per_suite,1}) -> false;
                                   ({end_per_suite,1}) -> false;
                                   ({_,1}) -> true;
                                   ({_,_}) -> false
                               end, Functions)].

%%--------------------------------------------------------------------
%% Function: init_per_suite(Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the suite.
%%
%% Description: Initialization before the suite.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    erlaws:start(),

    AwsKey = os:getenv("ERLAWS_TEST_AWS_KEY"),
    AwsSecKey = os:getenv("ERLAWS_TEST_AWS_SEC_KEY"),
    try
        case (AwsKey =/= false) andalso (AwsSecKey =/= false) of
            false ->
                throw({skip, "Environment variable ERLAWS_TEST_AWS_KEY and ERLAWS_TEST_AWS_SEC_KEY is not set"});
            _ -> ok
        end,
        AwsSecure =
            case os:getenv("ERLAWS_TEST_AWS_SECURE") of
                false -> true;
                "1" -> true;
                "true" -> true;
                _ -> false
            end,
        [{aws_key, AwsKey}, {aws_sec_key, AwsSecKey}, {secure, AwsSecure} | Config]
    catch
        {skip, _} = E0 ->
            ct:print(E0),
            E0
    end.


%%--------------------------------------------------------------------
%% Function: end_per_suite(Config0) -> void() | {save_config,Config1}
%%
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%%
%% Description: Cleanup after the suite.
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    ok.

%%--------------------------------------------------------------------
%% Function: init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%% Reason = term()
%%   The reason for skipping all test cases and subgroups in the group.
%%
%% Description: Initialization before each test case group.
%%--------------------------------------------------------------------
init_per_group(_group, Config) ->
    Config.


%%--------------------------------------------------------------------
%% Function: end_per_group(GroupName, Config0) ->
%%               void() | {save_config,Config1}
%%
%% GroupName = atom()
%%   Name of the test case group that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding configuration data for the group.
%%
%% Description: Cleanup after each test case group.
%%--------------------------------------------------------------------
end_per_group(_group, Config) ->
    Config.

%%--------------------------------------------------------------------
%% Function: init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%%
%% TestCase = atom()
%%   Name of the test case that is about to run.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for skipping the test case.
%%
%% Description: Initialization before each test case.
%%
%% Note: This function is free to add any key/value pairs to the Config
%% variable, but should NOT alter/remove any existing entries.
%%--------------------------------------------------------------------
init_per_testcase(TestCase, Config) ->
    AwsKey = proplists:get_value(aws_key, Config),
    AwsSecKey = proplists:get_value(aws_sec_key, Config),
    Secure = proplists:get_value(secure, Config),
    SQS = erlaws_sqs:new(AwsKey, AwsSecKey, Secure),
    [{sqs, SQS}|Config].

%%--------------------------------------------------------------------
%% Function: end_per_testcase(TestCase, Config0) ->
%%               void() | {save_config,Config1} | {fail,Reason}
%%
%% TestCase = atom()
%%   Name of the test case that is finished.
%% Config0 = Config1 = [tuple()]
%%   A list of key/value pairs, holding the test case configuration.
%% Reason = term()
%%   The reason for failing the test case.
%%
%% Description: Cleanup after each test case.
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, Config) ->
    Config.

test_erlaws_sqs() ->
    [{userdata,[{doc,"Testing the erlaws_sqs module"}]}].

test_1_basic(Config) ->
    C = proplists:get_value(sqs, Config),
    ct:print("C: ~p", [C]),

    %% Try illegal queue name
    {error, {"400", _}, _} = C:create_queue("bad*queue*name"),

    %% Create a new queue
    QueueName1 = lists:flatten(io_lib:format("test-~w-~w-~w-1", tuple_to_list(now()))),
    QueueName2 = lists:flatten(io_lib:format("test-~w-~w-~w-2", tuple_to_list(now()))),
    
    ct:print("QueueName: ~s ~s", [QueueName1, QueueName2]),
    {ok, QueueUrl1, _} = C:create_queue(QueueName1, 55), %% Set 55 for QueueName1
    {ok, QueueUrl2, _} = C:create_queue(QueueName2),     %% Default for QueueName2
    ct:print("QueueUrl: ~s ~s", [QueueUrl1, QueueUrl2]),

    %% Test get_queue_url
    {ok, QueueUrl1, _} = C:get_queue_url(QueueName1),
    {ok, QueueUrl2, _} = C:get_queue_url(QueueName2),

    %% Try a list_queues and see if it's really there
    {ok, QueueUrls1, _} = C:list_queues(QueueName1),
    ct:print("QueueUrls1: ~p", [QueueUrls1]),
    true = lists:member(QueueUrl1, QueueUrls1),

    {ok, QueueUrls2, _} = C:list_queues(QueueName2),
    ct:print("QueueUrls1: ~p", [QueueUrls2]),
    true = lists:member(QueueUrl2, QueueUrls2),

    %% Check visibility timeout
    {ok, AttrList1, _} = C:get_queue_attr(QueueUrl1),
    VisibilityTimeout1 = proplists:get_value("VisibilityTimeout", AttrList1),
    ct:print("AttrList: ~p", [AttrList1]),
    ct:print("VisibilityTimeout1: ~p", [VisibilityTimeout1]),

    {ok, AttrList2, _} = C:get_queue_attr(QueueUrl2),
    VisibilityTimeout2 = proplists:get_value("VisibilityTimeout", AttrList2),
    ct:print("AttrList: ~p", [AttrList2]),
    ct:print("VisibilityTimeout2: ~p", [VisibilityTimeout2]),

    %% Check visibility timeout of QueueName1
    {ok, AttrList1a, _} = C:get_queue_attr(QueueUrl1),
    VisibilityTimeout1a = proplists:get_value("VisibilityTimeout", AttrList1a),
    VisibilityTimeout1a = 55,

    %% Set and check visibility timeout of QueueName2
    {ok, _} = C:set_queue_attr(visibility_timeout, QueueUrl2, 45),
    {ok, AttrList2a, _} = C:get_queue_attr(QueueUrl2),
    VisibilityTimeout2a = proplists:get_value("VisibilityTimeout", AttrList2a),
    VisibilityTimeout2a = 45,

    %% Add a message
    MsgBody1 = "This is a test.\n",
    {ok, _Msg1, _} = C:send_message(QueueUrl1, MsgBody1),
    
    %% Read a message
    {ok, GotMsgs1, _} = C:receive_message(QueueUrl1),
    [GotMsg1] = GotMsgs1,
    MsgBody1 = GotMsg1#sqs_message.body,
    
    %% Another read, shouldn't find anything
    {ok, [], _} = C:receive_message(QueueUrl1),
    
    %% Delete the message
    {ok, _} = C:delete_message(QueueUrl1, GotMsg1#sqs_message.receiptHandle),

    %% Delete queues
    {ok, _} =  C:delete_queue(QueueUrl1),
    {ok, _} =  C:delete_queue(QueueUrl2),

    %% Try a list_queues and ensure it's not there
    {ok, QueueUrls99, _} =C:list_queues(),
    false = lists:member(QueueUrl1, QueueUrls99),
    false = lists:member(QueueUrl2, QueueUrls99).

