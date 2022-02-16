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
      |> Timer.restart_election_timer()
      |> Server.next()
  end # receive
end # start

# _________________________________________________________ next()
def next(s) do
  s = receive do

    # { :APPEND_ENTRIES_REQUEST, msg } ->
    #    # omitted

    # { :APPEND_ENTRIES_REPLY, msg } ->
    #   # omitted

    { :VOTE_REQUEST, { q, term } } ->
      Vote.request(s, q, term)

    { :VOTE_REPLY, { q, term, vote} } ->
      Vote.reply(s, q, term, vote)

    { :ELECTION_TIMEOUT, {curr_term, curr_election} } ->
      Vote.election_timeout(s, curr_term, curr_election)

    { :APPEND_ENTRIES_TIMEOUT, { q } } ->
      AppendEntries.timeout(s, q)

    # { :CLIENT_REQUEST, msg } ->
    #    # omitted

    # unexpected ->
    #   # omitted

  end # receive

  Server.next(s)

end # next

end # Server
