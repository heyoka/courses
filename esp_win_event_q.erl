%% Date: 05.01.17 - 17:41
%% Ⓒ 2019 heyoka
%% @doc rewrite of window with queue module
%%
-module(esp_win_event_q).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

-record(state, {
   every,
   period,
   window,
   length = 0,
   count = 0,
   at = 0
}).

options() ->
   [{period, integer, 12}, {every, integer, 4}].

init(NodeId, _Inputs, #{period := Period, every := Every}) ->
   io:format("~p init:node~n",[NodeId]),
   State = #state{every = Every, period = Period, window = queue:new()},
   {ok, all, State}.

process(_Inport, #data_point{} = Point, State=#state{} ) ->
   NewState = accumulate(Point, State),
   EvictState = maybe_evict(NewState),
   maybe_emit(EvictState).

handle_info(Request, State) ->
   io:format("~p request: ~p~n", [State, Request]),
   {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
accumulate(Point = #data_point, State = #state{window = Win, count = C, at = At}) ->
   State#state{count = C+1, at = At+1, window = queue:in(Point, Win)}.

maybe_emit(State = #state{every = Count, count = Count, window = Win}) ->
   Batch = #data_batch{points = queue:to_list(Win)},
   {emit, Batch, State#state{count = 0}};
maybe_emit(State = #state{}) ->
   {ok, State}.

maybe_evict(State = #state{period = Period, at = At, window = Win}) when At == Period+1 ->
   NewWin = queue:out(Win),
   State#state{at = Period, window = NewWin};
maybe_evict(Win = #esp_window{}) ->
   Win.