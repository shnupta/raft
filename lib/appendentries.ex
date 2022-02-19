
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
    s
      |> State.stepdown(term)
      |> State.leaderP(q)
      |> Debug.info("Stepped down and set leaderP as #{inspect q}", 3)
  else
    s
  end
  if term < s.curr_term do
    send q, { :APPEND_ENTRIES_REPLY, self(), { s.curr_term, false, -1} }
    Debug.sent(s, { :APPEND_ENTRIES_REPLY, self(), { s.curr_term, false, -1}, q}, 2)
  else
    success = prev_log_index == 0 or ((prev_log_index <= Log.last_index(s)) and Log.term_at(s, prev_log_index) == prev_log_term)
    {s, index} = if success do
      store_entries(s, prev_log_index, entries, commit_index)
    else
      {s, 0}
    end
    send q, { :APPEND_ENTRIES_REPLY, self(), { s.curr_term, success, index} }
    s
      |> Debug.sent({ :APPEND_ENTRIES_REPLY, self(), { s.curr_term, success, index}, q}, 2)
      |> apply_commits()
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

def reply(s, q, { term, success, index }) do
  if term > s.curr_term do
    State.stepdown(s, term)
  else
    s = if s.role == :LEADER and term == s.curr_term do
      s = if success do
        # We know entries up to index are replicated well,
        # hence update match_index.
        s
          |> State.next_index(q, index + 1)
          |> State.match_index(q, index)
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
    Debug.info(s, "Leader about to start checking for commits!", 2)
    check_commit(s)
  end
end # reply

defp check_commit(s) do
  cur_commit_index = s.commit_index
  uncommited = Enum.slice(s.log, cur_commit_index .. -1)
  s = Debug.info(s, "Uncommited Logs: #{inspect uncommited}", 2)
  new_commit_index =
      Enum.reduce_while(
        uncommited,
        cur_commit_index + 1,
        fn _entry, entry_i ->
          reps_count = known_replications(s, entry_i)
          if reps_count >= s.majority do
            {:cont, entry_i + 1} # Continue trying the next index.
          else
            {:halt, entry_i} # Return the current entry_index.
          end
        end
      ) - 1

  s = Debug.info(s, "Old commit index: #{inspect cur_commit_index}", 2)
  s = Debug.info(s, "New commit index: #{inspect new_commit_index}", 2)
  s = s
    |> State.commit_index(new_commit_index)
    |> apply_commits()


  # Grab the commits that were just applied and send reply to clients
  just_commited = Enum.slice(s.log, cur_commit_index .. new_commit_index - 1)
  s = Debug.info(s, "Just committed: #{inspect just_commited}", 3)
  for {_, entry} <- just_commited do
    # Send to entry.ClientP with :LEADER and our process id with mid.
    ClientReq.send_reply(entry)
  end
  s
end # check_commit

defp known_replications(s, index) do
  Enum.reduce(
    s.servers, 1,
    fn server, count ->
      if server != s.selfP and Map.get(s.match_index, server) >= index do
        count + 1
      else
        count
      end
    end
  )
end #known_replications

defp apply_commits(s) do
  if s.last_applied == s.commit_index do
    # Case where no new changes to apply
    Debug.info(s, "No new commits to apply", 3)
  else
    needs_apply = Enum.slice(s.log, s.last_applied .. s.commit_index - 1)
    for {_, entry} <- needs_apply do
      send s.databaseP, { :DB_REQUEST, entry.request }
    end
    s = Debug.info(s, "Just applied #{inspect needs_apply}", 3)
    State.last_applied(s, s.commit_index)
  end
end #apply_commits

end # AppendEntries
