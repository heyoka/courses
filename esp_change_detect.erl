%% Date: 06.06 - 19:04
%% Ⓒ 2019 heyoka
%% @doc emits new points only if different from the previous point
%% multiple fields can be monitored by this node
%% if reset_timeout is given, all previous values are resetted, if there are no points comming in for this amount of time
%%
-module(esp_change_detect).
-author("Alexander Minichmair").

%% API
-behavior(df_component).

-include("faxe.hrl").
%% API
-export([init/3, process/3, options/0, handle_info/2]).

-record(state, {
   node_id,
   fields,
   values = [],
   reset_timeout,
   timer
}).

options() -> [{fields, binary_list}, {reset_timeout, binary, undefined}].

init(_NodeId, _Ins, #{fields := FieldList, reset_timeout := Timeout} = P) ->
   lager:notice("~p init: ~p",[_NodeId, P]),
   Time = faxe_time:duration_to_ms(Timeout),
   {ok, all, #state{fields = FieldList, reset_timeout = Time}}.

process(_In, #data_batch{points = Points} = Batch,
    State = #state{fields = FieldNames, values = Vals, timer = TRef, reset_timeout = Time}) ->
   cancel_timer(TRef),
   {NewPoints, LastValues} = process_points(Points, [], Vals, FieldNames),
   NewState = State#state{values = LastValues, timer = reset_timeout(Time)},
   case NewPoints of
      [] -> {ok, NewState};
      Es when is_list(Es) -> {emit, Batch#data_batch{points = NewPoints}}
   end;
process(_Inport, #data_point{} = Point,
    State = #state{fields = Fields, values = LastValues, timer = TRef, reset_timeout = Time}) ->
   lager:notice("~p point in : ~p", [?MODULE, Point#data_point.fields]),
   cancel_timer(TRef),
   {Filtered, NewValues} = process_point(Point, LastValues, Fields),
   lager:info("new last values: ~p" ,[NewValues]),
   NewState = State#state{values = NewValues, timer = reset_timeout(Time)},
   case Filtered of
      [] -> {ok, NewState};
      E when is_list(E) -> lager:notice("~p emitting: ~p" ,[?MODULE, Filtered]),
         {emit, Point#data_point{fields = Filtered}, NewState}
   end.


handle_info(reset_timeout, State) ->
   lager:info("reset last values"),
   {ok, State#state{values = []}}.

process_points([], NewPoints, LastValues, _FieldNames) ->
   {NewPoints, LastValues};
process_points([P|RP], PointsAcc, LastValues, FieldNames) ->
   {NewFields, NewLastValues} = process_point(P, LastValues, FieldNames),
   process_points(RP, PointsAcc ++ [P#data_point{fields = NewFields}], NewLastValues, FieldNames).

process_point(Point = #data_point{fields = Fields}, [], FieldNames) ->
   {Fields, get_values(Point, FieldNames)};
process_point(Point = #data_point{}, LastValues, FieldNames) ->
   Filtered = filter(Point, LastValues),
   {Filtered, get_values(Point,FieldNames)}.

filter(#data_point{fields = Fields}, LastVals) ->
   lists:filter(fun({FName, FieldValue}) ->
                  case proplists:get_value(FName, LastVals) of
                     undefined -> true;
                     Val when Val == FieldValue -> false;
                     _ -> true
                  end
                end, Fields).

get_values(P = #data_point{}, FieldNames) ->
   lists:filter(fun(E) -> E /= undefined end, [{Field, flowdata:field(P, Field)} || Field <- FieldNames]).

reset_timeout(undefined) -> undefined;
reset_timeout(Time) -> lager:info("new timer: ~p" ,[Time]),erlang:send_after(Time, self(), reset_timeout).

cancel_timer(undefined) -> ok;
cancel_timer(TimerRef) when is_reference(TimerRef) -> lager:info("cancel timer: ~p" ,[TimerRef]), erlang:cancel_timer(TimerRef).

