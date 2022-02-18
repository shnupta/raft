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

    { :APPEND_ENTRIES_REQUEST, q, msg } ->
      s
        |> Debug.received({ :APPEND_ENTRIES_REQUEST, q, msg }, 3)
        |> AppendEntries.request(q, msg)

    { :APPEND_ENTRIES_REPLY, q, msg } ->
      s
        |> Debug.received({ :APPEND_ENTRIES_REPLY, q, msg }, 3)
        |> AppendEntries.reply(q, msg)

    { :VOTE_REQUEST, { q, term, last_log_term, last_log_index} } ->
      s
        |> Debug.received({ :VOTE_REQUEST, { q, term, last_log_term, last_log_index} }, 3)
        |> Vote.request(q, term, last_log_term, last_log_index)

    { :VOTE_REPLY, { q, term, vote} } ->
      s
        |> Debug.received({ :VOTE_REPLY, { q, term, vote} }, 3)
        |> Vote.reply(q, term, vote)

    { :ELECTION_TIMEOUT, {curr_term, curr_election} } ->
      s
        |> Debug.received({ :ELECTION_TIMEOUT, {curr_term, curr_election} }, 3)
        |> Vote.election_timeout(curr_term, curr_election)

    { :APPEND_ENTRIES_TIMEOUT, { term, q } } ->
      s
        |> Debug.received({ :APPEND_ENTRIES_TIMEOUT, { term, q } }, 3)
        |> AppendEntries.timeout(q)

    { :CLIENT_REQUEST, req } ->
      s
        |> Debug.received({ :CLIENT_REQUEST, req }, 3)
        |> ClientReq.request(req)

    # unexpected ->
    #   # omitted

  end # receive

  Server.next(s)

end # next

end # Server
