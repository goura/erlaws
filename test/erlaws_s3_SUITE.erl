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

    BucketName = lists:flatten(io_lib:format("test-~w-~w-~w", tuple_to_list(now()))),
    part_create_bucket(C, BucketName),
    timer:sleep(1000),
    try
        part_put_and_get_objects(C, BucketName)
    after
        part_delete_objects(C, BucketName),
        part_delete_bucket(C, BucketName)
    end.

test_list_1(Config) ->
    C = proplists:get_value(s3, Config),
    ct:print("C: ~p", [C]),

    BucketName = lists:flatten(io_lib:format("test-~w-~w-~w", tuple_to_list(now()))),
    part_create_bucket(C, BucketName),
    timer:sleep(1000),
    try
        part_put_and_get_objects(C, BucketName),
        part_list_contents(C, BucketName)
    after
        part_delete_objects(C, BucketName),
        part_delete_bucket(C, BucketName)
    end.


%%%%% Test parts %%%%%

part_create_bucket(S3, BucketName) ->
    %% Create a new, empty bucket
    ct:print("BucketName: ~s", [BucketName]),
    {ok, BucketName, _} = S3:create_bucket(BucketName),

    %% Try list_buckets and see if it's really there
    {ok, BucketNames, _} = S3:list_buckets(),
    true = lists:member(BucketName, BucketNames).

part_put_and_get_objects(S3, BucketName) ->
    %% Upload 01
    Data01 = <<"This is a test of file upload and download">>,
    {ok, _, _} = S3:put_object(BucketName, "foobar", Data01,
                               "text/plain", []),

    %% Upload 02
    Data02 = <<"Another test data">>,
    {ok, _, _} = S3:put_object(BucketName, "barbaz", Data02,
                               "text/plain", [{"name", "metavalue"}]),

    %% Download 01
    {ok, Data01, _} = S3:get_object(BucketName, "foobar"),

    %% Download 02
    {ok, Data02, _} = S3:get_object(BucketName, "barbaz"),

    %% Try to download not existing key
    {error, {"NoSuchKey", _, _}} = S3:get_object(BucketName, "bogus"),

    %% Info 01
    {ok, [], _} = S3:info_object(BucketName, "foobar"),

    %% Info 02
    {ok, [{"name", "metavalue"}], _} = S3:info_object(BucketName, "barbaz").

part_delete_objects(S3, BucketName) ->
    %% Delete 01
    {ok, _} = S3:delete_object(BucketName, "foobar"),

    %% Delete 02
    {ok, _} = S3:delete_object(BucketName, "barbaz"),

    %% Delete all
    {ok, {s3_list_result, _, Contents, _}, _} = S3:list_contents(BucketName),
    lists:map(fun(Elm)->
                      {s3_object_info, Path, _, _, _} = Elm,
                      S3:delete_object(BucketName, Path)
              end, Contents).


part_delete_bucket(S3, BucketName) ->
    %% Now delete bucket
    {ok, _} =  S3:delete_bucket(BucketName),

    %% Try list_buckets and ensure it's not there
    {ok, BucketNames99, _} =S3:list_buckets(),
    false = lists:member(BucketName, BucketNames99).

part_list_contents(S3, BucketName) ->
    %% Assuming part_create_bucket() and part_put_and_get_objects() were already called.
    {ok, {s3_list_result, _, Contents01, _}, _} = S3:list_contents(BucketName),
    2 = length(Contents01), % Ref part_put_and_get_objects/2

    lists:map(fun(Elm) ->
                {s3_object_info, Path01, _, _, _} = Elm, % Ignore Date, Etag and Size
                true = lists:member(Path01, ["foobar", "barbaz"])
              end, Contents01),

    %% Limit the number of results to 1
    {ok, {s3_list_result, _, Contents02, _}, _} = S3:list_contents(BucketName, [{max_keys, 1}]),
    1 = length(Contents02),
    [{s3_object_info, Path02, _, _, _}] = Contents02,
    true = lists:member(Path02, ["foobar", "barbaz"]),

    %% Add test data for prefix tests
    PData01 = <<"For prefix test 01">>,
    {ok, _, _} =
        S3:put_object(BucketName, "hahaha/fufufu", PData01, "text/plain", []),

    PData02 = <<"For prefix test 02">>,
    {ok, _, _} =
        S3:put_object(BucketName, "gagaga/fufufu", PData02, "text/plain", []),

    PData03 = <<"For prefix test 03">>,
    {ok, _, _} =
        S3:put_object(BucketName, "gagaga/lalala", PData03, "text/plain", []),

    PData04 = <<"For prefix test 04">>,
    {ok, _, _} =
        S3:put_object(BucketName, "gagaga/tatata", PData04, "text/plain", []),

    PData05 = <<"For prefix test 05">>,
    {ok, _, _} =
        S3:put_object(BucketName, "gagaga/gigigi/fafafa", PData05, "text/plain", []),

    PData06 = <<"For prefix test 06">>,
    {ok, _, _} =
        S3:put_object(BucketName, "gagaga/gigigi/fufufu", PData06, "text/plain", []),

    %% Prefix test
    {ok, {s3_list_result, _, Contents03, _}, _} = S3:list_contents(BucketName, [{prefix, "gagaga"}]),
    5 = length(Contents03),
    lists:map(fun(Elm) ->
                {s3_object_info, Path03, _, _, _} = Elm,
                true = lists:member(Path03, ["gagaga/fufufu", "gagaga/gigigi/fafafa", "gagaga/gigigi/fufufu", "gagaga/lalala", "gagaga/tatata"])
              end, Contents03),

    %% Prefix and max_keys
    {ok, {s3_list_result, _, Contents04, _}, _} = S3:list_contents(BucketName, [{prefix, "gagaga"}, {max_keys, 2}]),
    2 = length(Contents04),
    lists:map(fun(Elm) ->
                {s3_object_info, Path04, _, _, _} = Elm,
                true = lists:member(Path04, ["gagaga/fufufu", "gagaga/gigigi/fafafa"])
              end, Contents04),

    %% Prefix and marker
    {ok, {s3_list_result, _, Contents05, _}, _} = S3:list_contents(BucketName, [{prefix, "gagaga"}, {marker, "gagaga/gigigi/fafafa"}]),
    3 = length(Contents05),
    lists:map(fun(Elm) ->
                {s3_object_info, Path05, _, _, _} = Elm,
                true = lists:member(Path05, ["gagaga/gigigi/fufufu", "gagaga/lalala", "gagaga/tatata"])
              end, Contents05),

    %% Prefix and delimiter
    {ok, {s3_list_result, _, Contents06, Prefixes06}, _} = S3:list_contents(BucketName, [{prefix, "gagaga"}, {delimiter, "/"}]),
    0 = length(Contents06),
    1 = length(Prefixes06),
    ["gagaga/"] = Prefixes06,

    %% delimiter
    {ok, {s3_list_result, _, Contents07, Prefixes07}, _} = S3:list_contents(BucketName, [{delimiter, "/"}]),
    2 = length(Contents07),
    2 = length(Prefixes07),
    lists:map(fun(Elm) ->
                      {s3_object_info, Path07, _, _, _} = Elm,
                      true = lists:member(Path07, ["barbaz", "foobar"])
              end, Contents07),
    ["gagaga/", "hahaha/"] = Prefixes07.
