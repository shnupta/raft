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
      new_s = if term > s.curr_term do
        State.stepdown(s, term)
      else
        s
      end
      if (term == new_s.curr_term and (new_s.voted_for == q or new_s.voted_for == nil)) do
        voted_s = State.voted_for(new_s, q)
        |> Timer.restart_election_timer()
        send q, { :VOTE_REPLY, { self(), term, q}} # TODO: double check this
        voted_s
      else
        new_s
      end

    { :VOTE_REPLY, { q, term, vote} } ->
      new_s = if term > s.curr_term do
        State.stepdown(s, term)
      else
        s
      end

      if term == new_s.curr_term and new_s.role == :CANDIDATE do
        voted_s = if vote == self() do
          State.add_to_voted_by(new_s, q)
        else
          new_s
        end
        cancel_timer_s = Timer.cancel_append_entries_timer(voted_s, q)
        if State.has_majority_votes(cancel_timer_s) do
          # cancel_timer_s
          cancel_timer_s
            |> State.role(:LEADER)
            |> State.leaderP(self())
          # TODO: for each process except self, send append entries
          # And remove this below
          # leader_s
        else
          cancel_timer_s
        end
      else
        new_s
      end



    { :ELECTION_TIMEOUT, {curr_term, curr_election} } ->
      unless s.role == :LEADER do
        new_s = Timer.restart_election_timer(s)
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
        new_s
      end

    { :APPEND_ENTRIES_TIMEOUT, { q } } ->
      if s.role == :CANDIDATE do
        new_s = Timer.restart_append_entries_timer(s, q)
        send q, { :VOTE_REQUEST, { self(), new_s.curr_term } }
        Debug.sent(new_s, { :VOTE_REQUEST, { self(), new_s.curr_term } })
      end

    # { :CLIENT_REQUEST, msg } ->
    #    # omitted

    # unexpected ->
    #   # omitted

  end # receive

  Server.next(s)

end # next

end # Server
