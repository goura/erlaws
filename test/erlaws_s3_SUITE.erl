%% common_test suite for erlaws_s3

-module(erlaws_s3_SUITE).
-include_lib("common_test/include/ct.hrl").

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
    S3 = erlaws_s3:new(AwsKey, AwsSecKey, Secure),
    [{s3, S3}|Config].

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

test_erlaws_s3() ->
    [{userdata,[{doc,"Testing the erlaws_s3 module"}]}].

%%test_erlaws_s3(_Config) ->
%%    {skip,"Not implemented."}.

test_connection_1_basic(Config) ->
    C = proplists:get_value(s3, Config),
    ct:print("C: ~p", [C]),

    %% Create a new, empty bucket
    BucketName = lists:flatten(io_lib:format("test-~w-~w-~w", tuple_to_list(now()))),
    ct:print("BucketName: ~s", [BucketName]),
    {ok, BucketName, _} = C:create_bucket(BucketName),

    %% Try a list_buckets and see if it's really there
    {ok, BucketNames, _} = C:list_buckets(),
    true = lists:member(BucketName, BucketNames),
    
    %% Upload 01
    Data01 = <<"This is a test of file upload and download">>,
    {ok, _, _} =
        C:put_object(BucketName, "foobar", Data01, "text/plain", []),
    
    %% Upload 02
    Data02 = <<"Another test data">>,
    {ok, _, _} = 
        C:put_object(BucketName, "barbaz", Data02, "text/plain", [{"name", "metavalue"}]),
    
    %% Download 01
    {ok, Data01, _} =
        C:get_object(BucketName, "foobar"),
    
    %% Download 02
    {ok, Data02, _} =
        C:get_object(BucketName, "barbaz"),

    %% Try to download not existing key
    {error, {"NoSuchKey", _, _}} =
        C:get_object(BucketName, "bogus"),
    
    %% Info 01
    {ok, [], _} = C:info_object(BucketName, "foobar"),
    
    %% Info 02
    {ok, [{"name", "metavalue"}], _} = C:info_object(BucketName, "barbaz"),
    
    %% Try to delete before emptying
    {error, _} = C:delete_bucket(BucketName),
    
    %% Delete 01
    {ok, _} = C:delete_object(BucketName, "foobar"),
    
    %% Delete 02
    {ok, _} = C:delete_object(BucketName, "barbaz"),
        
    %% Now delete bucket
    {ok, _} =  C:delete_bucket(BucketName),

    %% Try a list_buckets and ensure it's not there
    {ok, BucketNames99, _} =C:list_buckets(),
    false = lists:member(BucketName, BucketNames99).

