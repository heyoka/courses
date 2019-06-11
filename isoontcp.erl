%%%-------------------------------------------------------------------
%%% @author heyoka
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%% read data via rfc1006/iso on tcp protocol
%%
%% To establish a connection to a S7 PLC there are 3 steps:
%%
%% 1) Connect to PLC on TCP port 102
%% 2) Connect on ISO layer (COTP Connect Request)
%% 3) Connect on S7comm layer (s7comm.param.func = 0xf0, Setup communication)
%% Step 1) uses the IP address of the PLC/CP.
%%
%% Step 2) uses as a destination TSAP of two bytes length.
%% The first byte of the destination TSAP codes the communication type (1=PG, 2=OP).
%% The second byte of the destination TSAP codes the rack and slot number: This is the position of the PLC CPU.
%% The slot number is coded in Bits 0-4, the rack number is coded in Bits 5-7.
%%
%% Step 3) is for negotiation of S7comm specific details (like the PDU size).
%%
%%% @end
%%% Created : 07. Jun 2019 09:55
%%%-------------------------------------------------------------------
-module(isoontcp).
-author("heyoka").

-behaviour(gen_statem).

%% API
-export([start_link/1]).

%% gen_statem callbacks
-export([
   init/1,
   format_status/2,
   handle_event/4,
   terminate/3,
   code_change/4,
   callback_mode/0
]).

-define(SERVER, ?MODULE).

%% ISO packets
-define(TPKT_HEADER, <<Vrsn:8, _Reserved:8, PacketLength:16>>).
%% TPDU part which is the same for every TDPU type
-define(TPDU_HEADER_COMMON, <<HeaderLength:8, Code:4>>).
-define(TPDU_DR,     << ?TPDU_HEADER_COMMON/binary, 0:4, 0:16, Source_Ref:16, Class:4, Options:4, Reason:8>>).
-define(TPDU_CR_CC,  << ?TPDU_HEADER_COMMON/binary, 0:4, 0:16, 0:16, Class:4, Options:4, Reason:8>>).
-define(TPDU_DT_ED,  << ?TPDU_HEADER_COMMON/binary, Credit=0:4, Tpdu_Nr_EOT:8>>).


-define(CONNECT_TIMEOUT, 5000).
-define(HOST, "127.0.0.1").
-define(PORT, 102).
-define(RACK, 0).
-define(SLOT, 2).
-define(LOCAL_TSAP, undefined).
-define(REMOTE_TSP, undefined).

-define(SEND_TIMEOUT, 3000).
%% gen_tcp socket options
-define(SOCK_OPTS, [{active, true}, {mode, raw}, {packet, tpkt}, {reuseaddr, true} {send_timeout, ?SEND_TIMEOUT}]).


%% handle common often used events
-define(HANDLE_COMMON,
   ?FUNCTION_NAME(T, C, D) -> handle_common(T, C, D)).

-record(state, {
   socket            = undefined :: undefined|port(),
   reading_vars      = []        :: list(tuple()),
   polling_interval  = 1000      :: non_neg_integer() %% in ms
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_statem process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(ReadValues) when is_list(ReadValues) ->
   gen_statem:start_link({local, ?SERVER}, ?MODULE, ReadValues, []).

%%%===================================================================
%%% gen_statem callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_statem is started using gen_statem:start/[3,4] or
%% gen_statem:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {CallbackMode, StateName, State} |
%%                     {CallbackMode, StateName, State, Actions} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init(ReadValues) ->
   {ok, disconnected, #state{reading_vars = ReadValues}, {state_timeout, 0, connect}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_statem when it needs to find out
%% the callback mode of the callback module.
%%
%% @spec callback_mode() -> atom().
%% @end
%%--------------------------------------------------------------------
callback_mode() ->
   state_functions.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Called (1) whenever sys:get_status/1,2 is called by gen_statem or
%% (2) when gen_statem terminates abnormally.
%% This callback is optional.
%%
%% @spec format_status(Opt, [PDict, StateName, State]) -> term()
%% @end
%%--------------------------------------------------------------------
format_status(_Opt, [_PDict, _StateName, _State]) ->
   Status = some_term,
   Status.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name.  If callback_mode is statefunctions, one of these
%% functions is called when gen_statem receives and event from
%% call/2, cast/2, or as a normal process message.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Actions} |
%%                   {stop, Reason, NewState} |
%%    				 stop |
%%                   {stop, Reason :: term()} |
%%                   {stop, Reason :: term(), NewData :: data()} |
%%                   {stop_and_reply, Reason, Replies} |
%%                   {stop_and_reply, Reason, Replies, NewState} |
%%                   {keep_state, NewData :: data()} |
%%                   {keep_state, NewState, Actions} |
%%                   keep_state_and_data |
%%                   {keep_state_and_data, Actions}
%% @end
%%--------------------------------------------------------------------
disconnected(state_timeout, connect, StateData) ->
   %% do something to connect the tcp socket here ...
   {ok, Socket} = connect(),
   {next_state, connected, StateData#state{socket = Socket}, {state_timeout, 0, connect_request}}.

connected(state_timeout, connect_request, StateData=#state{socket = Socket}) ->
   %% send a CR Packet to the peer
   ok = gen_tcp:send(Socket, ?TPDU_CR_CC),
   {next_state, wait_cc, StateData};
?HANDLE_COMMON.

wait_cc(info, {tcp, Socket, Data}, StateData) ->
   {next_state, s7comm, StateData, {state_timeout, 0, connect_request}};
wait_cc(info, CCPacket=#{connect := refused}, StateData) ->
   {stop, cc_refused};
?HANDLE_COMMON.

s7comm(state_timeout, connect_request, StateData=#state{socket = Socket}) ->
   %% create s7 negotiation req and send
   ok = gen_tcp:send(Socket, <<>>),
   {next_state, wait_s7comm, StateData};
?HANDLE_COMMON.

wait_s7comm(info, {tcp, Socket, Data}, StateData) ->
   %% now immediately poll for the first time
   {next_state, poll, StateData, {state_timeout, 0, poll}};
wait_s7comm(info, {tcp, Socket, Data}, StateData) ->
   {stop, s7comm_refused};
?HANDLE_COMMON.

poll(state_timeout, poll, StateData=#state{socket = Socket, reading_vars = Vars, polling_interval = T}) ->
   ok = gen_tcp:send(Socket, Vars),
   %% after 'polling_interval' ms poll again
   {next_state, wait_read_reply, StateData, {{timeout, poll}, T, poll}};
?HANDLE_COMMON.

wait_read_reply(info, {tcp, Socket, Data}, StateData=#state{polling_interval = T}) ->
   {next_state, poll, StateData}
?HANDLE_COMMON.

%%% handle common events
-spec handle_common(atom(), tuple(), any()) -> tuple().
handle_common(info, {tcp_closed, _Socket}, _StateData) ->
   {stop, normal};
handle_common(info, {tcp_error, Reason}, _StateData) ->
   {stop, Reason};
handle_common(info, {shutdown, Reason}, _StateData=#state{socket = Socket}) ->
   catch gen_tcp:close(Socket),
   {stop, Reason}.
%%--------------------------------------------------------------------
%% @private
%% @doc
%%
%% If callback_mode is handle_event_function, then whenever a
%% gen_statem receives an event from call/2, cast/2, or as a normal
%% process message, this function is called.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Actions} |
%%                   {stop, Reason, NewState} |
%%    				 stop |
%%                   {stop, Reason :: term()} |
%%                   {stop, Reason :: term(), NewData :: data()} |
%%                   {stop_and_reply, Reason, Replies} |
%%                   {stop_and_reply, Reason, Replies, NewState} |
%%                   {keep_state, NewData :: data()} |
%%                   {keep_state, NewState, Actions} |
%%                   keep_state_and_data |
%%                   {keep_state_and_data, Actions}
%% @end
%%--------------------------------------------------------------------
handle_event(_EventType, _EventContent, _StateName, State) ->
   NextStateName = the_next_state_name,
   {next_state, NextStateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_statem when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_statem terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
   ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
   {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
connect() ->
   gen_tcp:connect(?HOST, ?PORT, ?SOCK_OPTS, ?CONNECT_TIMEOUT).

build_iso_conn_request() ->
   Class = 0,
   <<Vrsn:8, _Reserved:8, PacketLength:16, HeaderLength:8, Code:4, 0:4, 0:16, 0:16, Class:4, Options:4, Reason:8>>