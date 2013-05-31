%% common_test suite for erlaws_sdb

-module(erlaws_sdb_SUITE).
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
    SDB = erlaws_sdb:new(AwsKey, AwsSecKey, Secure),
    [{sdb, SDB}|Config].

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

test_erlaws_sdb() ->
    [{userdata,[{doc,"Testing the erlaws_sdb module"}]}].

test_1_basic(Config) ->
    C = proplists:get_value(sdb, Config),
    ct:print("C: ~p", [C]),

    %% Try illegal queue name
    {error, {"InvalidParameterValue", _, _}} = C:create_domain("bad:domain:name"),

    %% Create a new domain
    DomainName = lists:flatten(io_lib:format("test_~w_~w_~w", tuple_to_list(now()))),
    
    ct:print("DomainName: ~s", [DomainName]),
    {ok, _} = C:create_domain(DomainName),

    try
        %% Put items and attributes
        SameValue = "same_value",
        {ok, _} =
            C:put_attributes(DomainName, "item1",
                             [{"name1", SameValue}, {"name2", "diff_value_1"}]),
        {ok, _} =
            C:put_attributes(DomainName, "item2",
                             [{"name1", SameValue}, {"name2", "diff_value_2"}]),
        
        %% Try to get the attributes and see if the match
        {ok, Results0} = C:get_attributes(DomainName, "item1", "", [{consistent_read, true}]),
        GotItem1 = proplists:get_value("item1", Results0),
        [SameValue] = proplists:get_value("name1", GotItem1),
        ["diff_value_1"] = proplists:get_value("name2", GotItem1),

        %% Try a search of two
        Query1 = lists:flatten(
                   io_lib:format("select * from ~s where name1=\"~s\"", [DomainName, SameValue])),
        {ok, Results1, _} = C:select(Query1, [{consistent_read, true}]),
        2 = length(Results1),

        Query2 = lists:flatten(
                   io_lib:format("select * from ~s where name2=\"diff_value_2\"", [DomainName])),
        {ok, Results2, _} = C:select(Query2, [{consistent_read, true}]),
        1 = length(Results2),
        
        %% Delete all attributes associated with item1 (pattern #1)
        {ok, _} = C:delete_attributes(DomainName, "item1", [{"name1", true}, {"name2", true}]),

        {ok, Results1a} = C:get_attributes(DomainName, "item1", "", [{consistent_read, true}]),
        GotItem1a = proplists:get_value("item1", Results1a),
        ct:print("~p~n", [GotItem1a]),
        0 = length(GotItem1a),

        %% Delete all attributes associated with item2 (pattern #2)
        {ok, _} = C:delete_attributes(DomainName, "item2", []),

        {ok, Results2a} = C:get_attributes(DomainName, "item2", "", [{consistent_read, true}]),
        GotItem2a = proplists:get_value("item2", Results2a),
        0 = length(GotItem2a),
        
        %% Try a batch put operation on the domain
        Item3 = [{"name3_1", "value3_1"},
                 {"name3_2", "value3_2"},
                 {"name3_3", ["value3_3_1", "value3_3_2"]}],
        Item4 = [{"name4_1", "value4_1"},
                 {"name4_2", ["value4_2_1", "value4_2_2"]},
                 {"name4_3", "value4_3"}],
        {ok, _} = C:batch_put_attributes(DomainName, [{"item3", Item3}, {"item4", Item4}]),
        
        {ok, Results3} = C:get_attributes(DomainName, "item3", "", [{consistent_read, true}]),
        GotItem3 = proplists:get_value("item3", Results3),
        ["value3_2"] = proplists:get_value("name3_2", GotItem3),

        %% Delete item3
        {ok, _} = C:delete_item(DomainName, "item3"),
        
        {ok, Results4} = C:get_attributes(DomainName, "item3", "", [{consistent_read, true}]),
        GotItem3a = proplists:get_value("item3", Results4),
        0 = length(GotItem3a),
        
        %% Delete some attributes associated with item4 (pattern #3)
        {ok, _} = C:delete_attributes(DomainName, "item4", ["name4_1", "name4_3"]),

        {ok, Results4a} = C:get_attributes(DomainName, "item4", "", [{consistent_read, true}]),
        GotItem4a = proplists:get_value("item4", Results4a),
        1 = length(GotItem4a),

        %% Delete some attributes associated with item4 (pattern #4)
        {ok, _} = C:delete_attributes(DomainName, "item4", [{"name4_2_1", "value4_2_2"}]),

        {ok, Results4b} = C:get_attributes(DomainName, "item4", "", [{consistent_read, true}]),
        GotItem4b = proplists:get_value("item4", Results4b),
        1 = length(GotItem4b)

    after
        %% Delete the domain
        {ok, _} =  C:delete_domain(DomainName),
        
        %% Try a list_queues and ensure it's not there
        {ok, DomainNames99, _, _} =C:list_domains(),
        false = lists:member(DomainName, DomainNames99)
    end.
