
# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2

defmodule AppendEntries do

# s = server process state (c.f. this/self)

def timeout(s, q) do
  if s.role == :CANDIDATE do
    s = Timer.restart_append_entries_timer(s, q)
    send q, { :VOTE_REQUEST, { self(), s.curr_term } }
    Debug.sent(s, { :VOTE_REQUEST, { self(), s.curr_term }, q }, 2)
  else
    s
  end
end # timeout

 # ... omitted

end # AppendEntriess
