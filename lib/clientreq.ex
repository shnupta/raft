
# distributed algorithms, n.dulay, 8 feb 2022
# coursework, raft consensus, v2

defmodule ClientReq do

# s = server process state (c.f. self/this)

def request(s, req) do
  if s.role == :LEADER do
    entry = %{term: s.curr_term, request: req}
    # TODO: Don't append entries which we already have
    #       Instead check the status of the request and then reply based on that
    #       Or do what we already do when append entries and notify the client later on
    s
      |> Log.append_entry(entry)
      |> AppendEntries.send_all_append_entries()
  else
    if s.leaderP != nil, do: send req.clientP, { :CLIENT_REPLY, {req.cid, :NOT_LEADER, s.leaderP } }
    s
  end
end

def send_reply(entry) do
  send entry.request.clientP, { :CLIENT_REPLY, { entry.request.cid, :OK, self()} }
end

end # Clientreq
