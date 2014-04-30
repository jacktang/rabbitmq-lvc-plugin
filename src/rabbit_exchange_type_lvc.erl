-module(rabbit_exchange_type_lvc).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_lvc_plugin.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/2]).
-export([validate/1, validate_binding/2,
         create/2, delete/3, policy_changed/2, add_binding/3,
         remove_bindings/3, assert_args_equivalence/2]).

-export([match_route_key/2]).

description() ->
    [{name, <<"lvc">>},
     {description, <<"Last-value cache exchange.">>}].

serialise_events() -> false.

route(Exchange = #exchange{name = Name},
      Delivery = #delivery{message = #basic_message{
                             routing_keys = RKs,
                             content = Content
                            }}) ->
    Keys = case RKs of
               CC when is_list(CC) -> CC;
               To                 -> [To]
           end,
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              [mnesia:write(?LVC_TABLE,
                            #cached{key = #cachekey{exchange=Name,
                                                    routing_key=K},
                                    content = Content},
                            write) ||
                  K <- Keys]
      end),
    rabbit_exchange_type_topic:route(Exchange, Delivery).

validate(X) -> 
    rabbit_exchange_type_topic:validate(X).
validate_binding(X, B) -> 
    rabbit_exchange_type_topic:validate_binding(X, B).
create(Tx, X) -> 
    rabbit_exchange_type_topic:create(Tx, X).

delete(transaction, #exchange{ name = Name } = X, Bs) ->
    [mnesia:delete(?LVC_TABLE, K, write) ||
        #cached{ key = K } <-
            mnesia:match_object(?LVC_TABLE,
                                #cached{key = #cachekey{
                                          exchange = Name, _ = '_' },
                                        _ = '_'}, write)],
    rabbit_exchange_type_topic:delete(transaction, X, Bs);
delete(Tx, X, Bs) ->
    rabbit_exchange_type_topic:delete(Tx, X, Bs).

policy_changed(X1, X2) ->
    rabbit_exchange_type_topic:policy_changed(X1, X2).

add_binding(none, #exchange{ name = XName } = X,
            #binding{ key = RoutingKey,
                      destination = QueueName } = B) ->
    case rabbit_amqqueue:lookup(QueueName) of
        {error, not_found} ->
            rabbit_misc:protocol_error(
              internal_error,
              "could not find queue '~s'",
              [QueueName]);
        {ok, #amqqueue{ pid = Q }} ->
            case lists:member($#, binary_to_list(RoutingKey)) or 
                lists:member($*, binary_to_list(RoutingKey)) of
                false ->
                    case mnesia:dirty_read(
                           ?LVC_TABLE,
                           #cachekey{exchange=XName,
                                     routing_key=RoutingKey }) of
                        [] ->
                            ok;
                        [#cached{content = Content}] ->
                            {Props, Payload} =
                                rabbit_basic:from_content(Content),
                            Msg = rabbit_basic:message(
                                    XName, RoutingKey, Props, Payload),
                            rabbit_amqqueue:deliver(
                              Q, rabbit_basic:delivery(false, false, Msg, undefined))
                    end;
                true ->
                    Caches = mnesia:dirty_match_object(
                               ?LVC_TABLE, #cachekey{exchange = XName, routing_key = '_'}),
                    lists:foreach(
                      fun(#cached{key = #cachekey{routing_key = RKey}, content = Content}) ->
                              case match_route_key(RKey, RoutingKey) of
                                  true ->
                                      {Props, Payload} =
                                          rabbit_basic:from_content(Content),
                                      Msg = rabbit_basic:message(
                                              XName, RoutingKey, Props, Payload),
                                      rabbit_amqqueue:deliver(
                                        Q, rabbit_basic:delivery(
                                             false, false, Msg, undefined));
                                  false ->
                                      ok
                              end
                      end, Caches)
            end
    end,
    rabbit_exchange_type_topic:add_binding(none, X, B);
add_binding(Tx, X, B) ->
    rabbit_exchange_type_topic:add_binding(Tx, X, B).

remove_bindings(Tx, X, Bs) -> 
    rabbit_exchange_type_topic:remove_bindings(Tx, X, Bs).

assert_args_equivalence(X, Args) ->
    rabbit_exchange_type_topic:assert_args_equivalence(X, Args).

match_route_key(Source, Dest) ->
    SourceWords = split_topic_key(Source),
    DestWords = split_topic_key(Dest),
    match_words(SourceWords, DestWords).

match_words([], []) ->
    true;
match_words([W|TS], [W|TD]) ->
    match_words(TS, TD);
match_words([_|TS], ["*"|TD]) ->
    match_words(TS, TD);
match_words(WS, ["#"|TD]) ->
    Len = length(WS),
    lists:any(
      fun(N) ->
              NWS = lists:nthtail(N, WS),
              match_words(NWS, TD)
      end, lists:seq(0, Len - 1));
match_words(_Source, _Dest) ->
    false.

split_topic_key(Key) ->
    split_topic_key(Key, [], []).

split_topic_key(<<>>, [], []) ->
    [];
split_topic_key(<<>>, RevWordAcc, RevResAcc) ->
    lists:reverse([lists:reverse(RevWordAcc) | RevResAcc]);
split_topic_key(<<$., Rest/binary>>, RevWordAcc, RevResAcc) ->
    split_topic_key(Rest, [], [lists:reverse(RevWordAcc) | RevResAcc]);
split_topic_key(<<C:8, Rest/binary>>, RevWordAcc, RevResAcc) ->
    split_topic_key(Rest, [C | RevWordAcc], RevResAcc).
