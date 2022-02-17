
# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2

defmodule AppendEntries do

# s = server process state (c.f. this/self)

def timeout(s, q) do
  s = if s.role == :CANDIDATE do
    s = Timer.restart_append_entries_timer(s, q)
    send q, { :VOTE_REQUEST, { self(), s.curr_term } }
    Debug.sent(s, { :VOTE_REQUEST, { self(), s.curr_term }, q }, 2)
  else
    s
  end

  if s.role == :LEADER do
    Debug.info(s, "Leader sending append entries request to #{inspect q}", 4)
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
  last_entry = min(Log.last_index(s), s.next_index[q] + 1)
  Debug.info(s, "Last entry = #{last_entry}")

  entries = Enum.slice(s.log, s.next_index[q], last_entry - Log.last_index(s))
  send q, { :APPEND_ENTRIES_REQUEST, self(),
  { s.curr_term, prev_log_index, prev_log_term,
  entries, s.commit_index } }

  Debug.sent(s, { :APPEND_ENTRIES_REQUEST,
  { s.curr_term, prev_log_index, prev_log_term,
  entries, s.commit_index }, q }, 2)
  s
end # send_append_entries

def request(s, q, { curr_term, prev_log_index, prev_log_term, entries, commit_index }) do
  # TODO: this function next
  Timer.restart_election_timer(s)
end

end # AppendEntriess
