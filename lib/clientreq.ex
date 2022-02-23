
# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2

defmodule ClientReq do

# s = server process state (c.f. self/this)

def request(s, req) do
  if s.role == :LEADER do
    case Log.request_status(s, req.cid) do
      {:NEW, _} ->
        entry = %{term: s.curr_term, request: req}
        s
          |> Log.append_entry(entry)
          |> AppendEntries.send_all_append_entries()
      {:COMMITTED, old_entry} ->
        send_reply(old_entry)
        s
      {:LOGGED, _} ->
        s #do nothing
    end
  else
    if s.leaderP != nil, do: send req.clientP, { :CLIENT_REPLY, {req.cid, :NOT_LEADER, s.leaderP } }
    s
  end
end

def send_reply(entry) do
  send entry.request.clientP, { :CLIENT_REPLY, { entry.request.cid, :OK, self()} }
end

end # Clientreq
