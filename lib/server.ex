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
      s = if term > s.curr_term do
        State.stepdown(s, term)
          |> Debug.info(":VOTE_REQUEST - Stepping down", 2)
      else
        s
      end

      if (term == s.curr_term and (s.voted_for == q or s.voted_for == nil)) do
        s = State.voted_for(s, q)
        |> Timer.restart_election_timer()
        send q, { :VOTE_REPLY, { self(), term, s.voted_for}}
        Debug.sent(s, { :VOTE_REPLY, { self(), term, s.voted_for}, q}, 2)
        s
      else
        s
      end

    { :VOTE_REPLY, { q, term, vote} } ->
      s = if term > s.curr_term do
        State.stepdown(s, term)
          |> Debug.info(":VOTE_REPLY - Stepping down", 2)
      else
        s
      end

      if term == s.curr_term and s.role == :CANDIDATE do
        s = if vote == self() do
          State.add_to_voted_by(s, q)
        else
          s
        end

        s = Timer.cancel_append_entries_timer(s, q)

        if State.has_majority_votes(s) do
          s = s
            |> State.role(:LEADER)
            |> State.leaderP(self())
            |> Debug.info("Became leader", 2)
          # TODO: for each process except self, send append entries

          s
        else
          s
        end

      else
        s
      end


    { :ELECTION_TIMEOUT, {curr_term, curr_election} } ->
      if s.role != :LEADER do
        s = Timer.restart_election_timer(s)
          |> State.inc_election()
          |> State.inc_term()
          |> State.role(:CANDIDATE)
          |> State.vote_for_self()
          |> Debug.info("Cancelling all append entries timers")
          |> Timer.cancel_all_append_entries_timers()

        for q <- s.servers do
          send self(), { :APPEND_ENTRIES_TIMEOUT, { q } }
          Debug.sent(s, { :APPEND_ENTRIES_TIMEOUT, { q } })
        end
        s
      else
        s
      end

    { :APPEND_ENTRIES_TIMEOUT, { q } } ->
      if s.role == :CANDIDATE do
        s = Timer.restart_append_entries_timer(s, q)
        send q, { :VOTE_REQUEST, { self(), s.curr_term } }
        Debug.sent(s, { :VOTE_REQUEST, { self(), s.curr_term }, q }, 2)
      else
        s
      end

    # { :CLIENT_REQUEST, msg } ->
    #    # omitted

    # unexpected ->
    #   # omitted

  end # receive

  Server.next(s)

end # next

# defp sendAppendEntries

end # Server
