
# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2

defmodule AppendEntries do

# s = server process state (c.f. this/self)

def timeout(s, q) do
  s = if s.role == :CANDIDATE do
    s = Timer.restart_append_entries_timer(s, q)
    send q, { :VOTE_REQUEST, { self(), s.curr_term, Log.last_term(s), Log.last_index(s) } }
    Debug.sent(s, { :VOTE_REQUEST, { self(), s.curr_term, Log.last_term(s), Log.last_index(s) }, q }, 1)
    s
  else
    s
  end

  if s.role == :LEADER do
    Debug.info(s, "Leader sending append entries request to #{inspect q}", 3)
    AppendEntries.send_append_entries(s, q)
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
  Debug.info(s, "Last entry = #{last_entry}", 2)
  Debug.info(s, "Last index = #{Log.last_index(s)}", 2)

  entries = Enum.slice(s.log, last_entry..- 1)
  send q, { :APPEND_ENTRIES_REQUEST, self(),
  { s.curr_term, prev_log_index, prev_log_term,
  entries, s.commit_index } }

  Debug.sent(s, { :APPEND_ENTRIES_REQUEST,
  { s.curr_term, prev_log_index, prev_log_term,
  entries, s.commit_index }, q }, 2)
  s
end # send_append_entries

def request(s, q, { term, prev_log_index, prev_log_term, entries, commit_index }) do
  s = Timer.restart_election_timer(s)
  s = if term > s.curr_term do
    State.stepdown(s, term)
  else
    s
  end
  if term < s.curr_term do
    send q, { :APPEND_ENTRIES_REPLY, self(), { s.curr_term, false, -1} } # TODO: Check this index is allowed
    s
  else
    index = 0
    success = prev_log_index == 0 or ((prev_log_index <= Log.last_index(s)) and Log.term_at(s, prev_log_index) == prev_log_term)
    s = if success do
      {s, index} = store_entries(s, prev_log_index, entries, commit_index) # TODO: Implement this function
      s
    else
      s
    end
    send q, { :APPEND_ENTRIES_REPLY, self(), { s.curr_term, success, index} }
    s
  end
end

defp store_entries(s, prev_log_index, entries, commit_index) do
  {s, prev_log_index}
end

def reply(s, q, { term, success, index }) do # Implement handling the other servers' replies to append entry
  s
end

end # AppendEntriess
