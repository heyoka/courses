%% Date: 05.01.17 - 17:41
%% Ⓒ 2017 heyoka
%% @doc window which refers it's timing to the timestamp contained in the incoming data-items
%% rewrite with queue module instead of lists
%% @todo setup timeout where points get evicted even though there a timestamps missing ?
-module(esp_win_time_q).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

-record(state, {
   every,
   period,
   window,
   at,
   mark
}).

options() ->
   [{period, binary}, {every, binary}].

init(NodeId, _Inputs, #{period := Period, every := Every} = Params) ->
   io:format("~p init:node ~p~n",[NodeId, Params]),
   Ev = faxe_time:duration_to_ms(Every),
   Per = faxe_time:duration_to_ms(Period),
   State = #state{period = Per, every = Ev},
   {ok, all, State}.


process(_Inport, #data_point{ts = Ts} = Point, State=#state{} ) ->
   State1 = tick(State),
   NewState = accumulate(Point, State1),
   {ok, NewState}.

handle_info(Request, State) ->
   io:format("~p request: ~p~n", [State, Request]),
   {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

accumulate(Point = #data_point{ts = Ts}, State = #state{mark = undefined}) ->
   accumulate(Point, State#state{mark = Ts});
accumulate(Point = #data_point{ts = Ts}, State = #state{window = Win}) ->
   State#state{at = Ts, window = queue:in(Point, Win)}.

tick(State = #state{mark = undefined}) ->
   State;
tick(State = #state{mark = Mark, at = At, window = Win, period = Interval, every = Every}) ->
   ok;

%% tick the window
tick(State = #esp_window{stats = #esp_win_stats{events = {[],[],[]} } }) ->
   State;
tick(State = #esp_window{agg_mod = _Module, agg = _Agg, every = Every, agg_fields = _Field,
   stats = #esp_win_stats{events = {_TimeStamps, _Values, _E} = Events, at = At, mark = Mark} = Stats, period = Interval}) ->

   % on a tick, we check for sliding out old events
   {{Tss, _Vs, Es}=Keep, _Evict} = evict(Events, At, Interval),
   NewAt = lists:last(Tss),
%%   lager:notice("on tick timespan is ~p | At-Mark is: ~p | AT : ~p",[NewAt-hd(Tss), NewAt-Mark, faxe_time:to_date(NewAt)]),
   case (NewAt - Mark) >= Every of
      true -> %{ok, Res, AggS} = Module:emit(Stats, NewAgg),
%%         Res = c_agg:call({Tss, Vs}, Module, Agg, Field),
         Batch = #data_batch{points = Es},
%%         lager:warning("~n when ~p ~p emitting: ~p",[NewAt-Mark, ?MODULE, {Batch, length(Batch#data_batch.points)}]),
         dataflow:emit(Batch),
         State#esp_window{stats = Stats#esp_win_stats{events = Keep, mark = NewAt, at = NewAt}};
      false ->
         State#esp_window{stats = Stats#esp_win_stats{events = Keep, at = NewAt}}
   end.

%%%% filter list and evict old entries, with early abandoning
-spec evict(window_events(), non_neg_integer(), non_neg_integer()) -> {window_events(), list()}.
evict({Ts, Vals, Events}, At, Interval) ->
   {Keep, Evict} = win_util:split(Ts, At - Interval),
%%   lager:info("Evicted: ~p",[Evict]),
   { {Keep,  win_util:sync(Vals, Evict), win_util:sync(Events, Evict)},  Evict}
.

