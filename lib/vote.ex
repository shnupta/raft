
# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2

defmodule Vote do

# s = server process state (c.f. self/this)

def request(s, q, term) do
  s = if term > s.curr_term do
    State.stepdown(s, term)
      |> Debug.info(":VOTE_REQUEST - Stepping down", 2)
  else
    s
  end

  if (term == s.curr_term and (s.voted_for == q or s.voted_for == nil)) do
    s = State.voted_for(s, q)
    |> Timer.restart_election_timer()
    |> Debug.info("Sending my vote reply and restarting election timer", 2)
    send q, { :VOTE_REPLY, { self(), term, s.voted_for}}
    Debug.sent(s, { :VOTE_REPLY, { self(), term, s.voted_for}, q}, 2)
    s
  else
    s
  end
end # request

def reply(s, q, term, vote) do
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
      s
        |> State.role(:LEADER)
        |> State.leaderP(self())
        |> Debug.info("Became leader", 2)
        |> AppendEntries.send_all_append_entries()
        # |> Server.send_heartbeats()
    else
      s
    end

  else
    s
  end
end # reply

def election_timeout(s, _curr_term, _curr_election) do
  if s.role != :LEADER do
    s = Timer.restart_election_timer(s)
      |> State.inc_election()
      |> State.inc_term()
      |> State.role(:CANDIDATE)
      |> State.vote_for_self()
      |> Debug.info("Cancelling all append entries timers")
      |> Timer.cancel_all_append_entries_timers()

    for q <- s.servers do
      send self(), { :APPEND_ENTRIES_TIMEOUT, { s.curr_term, q } }
      Debug.sent(s, { :APPEND_ENTRIES_TIMEOUT, { s.curr_term, q } })
    end
    s
  else
    s
  end
end # election_timeout


# ... omitted

end # Vote
