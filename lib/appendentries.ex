
# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2

defmodule AppendEntries do

# s = server process state (c.f. this/self)

def timeout(s, q) do
  s = if s.role == :CANDIDATE do
    s = Timer.restart_append_entries_timer(s, q)
    send q, { :VOTE_REQUEST, { self(), s.curr_term, Log.last_term(s), Log.last_index(s) } }
    Debug.sent(s, { :VOTE_REQUEST, { self(), s.curr_term, Log.last_term(s), Log.last_index(s) }, q }, 1)
  else
    s
  end

  if s.role == :LEADER do
    s
      |> Debug.info("Leader sending append entries request to #{inspect q}", 3)
      |> AppendEntries.send_append_entries(q)
  else
    s
  end
end # timeout

def send_all_append_entries(s) do
  servers = Enum.filter(s.servers, fn p -> p != self() end)
  Enum.reduce(servers, s, fn q, s -> AppendEntries.send_append_entries(s, q) end)
end

def send_append_entries(s, q) do
  s = s
    |> Timer.restart_append_entries_timer(q)

  prev_log_index = s.next_index[q] - 1
  prev_log_term = Log.term_at(s, prev_log_index)
  last_entry = min(Log.last_index(s), prev_log_index)
  s = s
    |> Debug.info("Last entry = #{last_entry}", 4)
    |> Debug.info("Last index = #{Log.last_index(s)}", 4)

  entries = Enum.slice(s.log, last_entry..- 1)
  send q, { :APPEND_ENTRIES_REQUEST, self(),
  { s.curr_term, prev_log_index, prev_log_term,
  entries, s.commit_index } }

  s = Debug.sent(s, { :APPEND_ENTRIES_REQUEST,
  { s.curr_term, prev_log_index, prev_log_term,
  entries, s.commit_index }, q }, 2)
  s
end # send_append_entries

def request(s, q, { term, prev_log_index, prev_log_term, entries, commit_index }) do
  s = Timer.restart_election_timer(s)
  s = if term >= s.curr_term do
    State.stepdown(s, term)
  else
    s
  end
  if term < s.curr_term do
    send q, { :APPEND_ENTRIES_REPLY, self(), { s.curr_term, false, -1} } # TODO: Check this index is allowed
    Debug.sent(s, { :APPEND_ENTRIES_REPLY, self(), { s.curr_term, false, -1}, q}, 2)
  else
    success = prev_log_index == 0 or ((prev_log_index <= Log.last_index(s)) and Log.term_at(s, prev_log_index) == prev_log_term)
    {s, index} = if success do
      store_entries(s, prev_log_index, entries, commit_index)
    else
      {s, 0}
    end
    send q, { :APPEND_ENTRIES_REPLY, self(), { s.curr_term, success, index} }
    Debug.sent(s, { :APPEND_ENTRIES_REPLY, self(), { s.curr_term, success, index}, q}, 2)
  end
end

# TODO: Print some debug info to show the updated commit index
defp store_entries(s, prev_log_index, entries, commit_index) do
  { agreed, _ } = Enum.split(s.log, prev_log_index)
  s = s
    |> Log.new(Map.new(agreed ++ entries))
    |> State.commit_index(min(commit_index, Log.last_index(s)))
  {s, Log.last_index(s)}
end # store_entries

# TODO: Update the match index
# TODO: Find out how the leader updates it's commit index
def reply(s, q, { term, success, index }) do
  if term > s.curr_term do
    State.stepdown(s, term)
  else
    if s.role == :LEADER and term == s.curr_term do
      s = if success do
        State.next_index(s, q, index + 1)
      else
        State.next_index(s, q, max(1, s.next_index[q] - 1))
      end
      if s.next_index[q] <= Log.last_index(s) do
        send_append_entries(s, q)
      else
        s
      end
    else
      s
    end
  end
end # reply

end # AppendEntriess
