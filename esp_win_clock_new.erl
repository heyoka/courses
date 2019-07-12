%% Date: 05.01.17 - 17:41
%% Ⓒ 2019 heyoka
%% @doc this window-type has wall-clock timing
%%
-module(esp_win_clock_new).
-author("Alexander Minichmair").

-behaviour(df_component).

-include("faxe.hrl").

%% API
-export([init/3, process/3, handle_info/2, options/0]).

-record(state, {
   period,
   every,
   align,
   mark,
   fill_period,
   tick_time,
   window,
   log = [],
   has_emitted = false
}).

options() ->
   [{period, binary}, {every, binary}, {align, is_set, true}, {fill_period, is_set, false}].

init(NodeId, _Inputs, #{period := Period, every := Every, align := Align, fill_period := Fill}) ->
   NUnit =
      case Align of
         false -> false;
         true -> faxe_time:binary_to_duration(Every)
      end,
   io:format("~p init:node~n",[NodeId]),
   Every1 = faxe_time:duration_to_ms(Every),
   Per = faxe_time:duration_to_ms(Period),
   State =
      #state{period = Per, every = Every1, align = NUnit,
         fill_period = Fill, window = queue:new()},

   SendTimeout = send_timeout(NUnit, Every1),
   erlang:send_after(SendTimeout, self(), emit),
   {ok, all, State}.


process(_Inport, #data_point{ts = _Ts} = Point, State=#state{}) ->
   NewState = accumulate(Point, State),
   {ok, NewState}
.

handle_info(emit, State=#state{every = Every, align = Align}) ->
   NewState = emit(State),
   erlang:send_after(send_timeout(Align, Every), self(), emit),
   {ok, NewState}
;
handle_info(Request, State) ->
   io:format("~p request: ~p~n", [State, Request]),
   {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================
accumulate(Point=#data_point{ts = NewTs}, State = #state{log = [], mark = undefined, align = Align, window = Win}) ->
%%   NewTime = faxe_time:now(),
   Mark = new_ts(NewTs, Align),
   lager:notice("Initial mark is at: ~p ~n while data_points timestamp is at : ~p", [faxe_time:to_htime(Mark), faxe_time:to_htime(NewTs)]),
   State#state{log = [NewTs], window = queue:in(Point, Win), mark = Mark};
accumulate(Point=#data_point{}, State = #state{log = Log, window = Win}) ->
   NewTime = faxe_time:now(),
   State#state{log = Log ++ [NewTime], window = queue:in(Point, Win)}.


emit(State = #state{log = []}) ->
   State;
emit(State = #state{log = Log, mark = Mark , period = Interval, window = Window, fill_period = Fill, has_emitted = Emitted}) ->
   NewAt = faxe_time:now(),
   lager:info("Emit AT: ~p", [faxe_time:to_htime(NewAt)]),
   % on a tick, we check for sliding out old events
   {KeepLog, NewWindow, HasEvicted} = evict(Log, Window, NewAt, Interval),
   NewState =
   case (Fill == false) orelse (Fill == true andalso (Emitted == true orelse HasEvicted == true)) of
      true ->
         Batch = #data_batch{points = queue:to_list(NewWindow)},
         lager:warning("~n when ~p period: ~p emitting: ~p",[NewAt-Mark, Interval, length(Batch#data_batch.points)]),
         dataflow:emit(Batch),
         State#state{has_emitted = true};
      false -> State
   end,
   NewState#state{mark = NewAt, log = KeepLog, window = NewWindow}.

%%%% filter list and evict old entries, with early abandoning
%%%% since the list is ordered by time, the abandoning will work as expected
%%%% as soon as a timestamp falls into the bounderies of the window, the loop is stopped
%%%% and the rest of the list is returned, as is for Keep
-spec evict(list(), window_events(), non_neg_integer(), non_neg_integer()) -> {window_events(), list()}.
evict(Log, Window, At, Interval) ->
   {KeepTimestamps, Evict} = win_util:split(Log, At - Interval),
   lager:info("evict: [~p] ~p~n keep [~p]: ~p",[length(Evict), [faxe_time:to_date(E) || E <- Evict],
      length(KeepTimestamps), [faxe_time:to_date(T) || T <- KeepTimestamps]]),
   {KeepTimestamps, win_util:sync_q(Window, Evict), length(Evict) > 0}
.

new_ts(Ts, false) ->
   Ts;
new_ts(Ts, Align) ->
   faxe_time:align(Ts, Align).

send_timeout(false, Every) ->
   Every;
send_timeout(Align, Every) ->
   Now = faxe_time:now(),
   T = Every - (Now - faxe_time:align(Now, Align)),
   lager:notice("NOW is : ~p next emit should be at: ~p" ,[faxe_time:to_htime(Now), faxe_time:to_htime(Now+T)]),
   T.