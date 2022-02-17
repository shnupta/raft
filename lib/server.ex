# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2

defmodule Server do

# s = server process state (c.f. self/this)

# _________________________________________________________ Server.start()
def start(config, server_num) do
  config = config
    |> Configuration.node_info("Server", server_num)
    |> Debug.node_starting()

  receive do
  { :BIND, servers, databaseP } ->
    State.initialise(config, server_num, servers, databaseP)
      |> Debug.info(servers, 4)
      |> State.init_next_index()
      |> State.init_match_index()
      |> Timer.restart_election_timer()
      |> Server.next()
  end # receive
end # start

# _________________________________________________________ next()
def next(s) do
  s = receive do

    # { :HEARTBEAT_TIMEOUT } -> send_heartbeats(s)

    # { :HEARTBEAT } ->
    #   Debug.info(s, "Received heartbeat", 2)
    #   Timer.restart_election_timer(s)

    { :APPEND_ENTRIES_REQUEST, q, msg } ->
      AppendEntries.request(s, q, msg)

    # { :APPEND_ENTRIES_REPLY, msg } ->
    #   # omitted

    { :VOTE_REQUEST, { q, term } } ->
      Vote.request(s, q, term)

    { :VOTE_REPLY, { q, term, vote} } ->
      Vote.reply(s, q, term, vote)

    { :ELECTION_TIMEOUT, {curr_term, curr_election} } ->
      Vote.election_timeout(s, curr_term, curr_election)

    { :APPEND_ENTRIES_TIMEOUT, { _term, q } } ->
      AppendEntries.timeout(s, q)

    # { :CLIENT_REQUEST, req } ->
    #    ClientReq.request(s, req)

    # unexpected ->
    #   # omitted

  end # receive

  Server.next(s)

end # next

# def send_heartbeats(s) do
#   servers = Enum.filter(s.servers, fn p -> p != self() end)
#   for s <- servers, do: send s, { :HEARTBEAT }
#   Process.send_after(self(), { :HEARTBEAT_TIMEOUT }, 50)
#   s
# end

end # Server
