
# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2

defmodule ClientReq do

# s = server process state (c.f. self/this)

def request(s, req) do
  Debug.received(s, req, 1)
  if s.role == :LEADER do
    entry = %{term: s.curr_term, request: req}
    s = s
      |> Log.append_entry(entry)
      |> AppendEntries.send_all_append_entries()
    # send req.clientP, { :CLIENT_REPLY, { m_cid, reply, self() }}
    s
  else
    s
  end
end

end # Clientreq
