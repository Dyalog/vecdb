:Namespace vecdbclt

    (⎕IO ⎕ML ⎕WX)←1 0 3

    ∇ r←CltLock resource
     ⍝ Cover-function for call to Lock from a Client
     
      r←Lock CONNECTION resource
    ∇

    ∇ r←CltRelease resource
     ⍝ Cover-function for call to Lock from a Client
     
      r←Release CONNECTION resource
    ∇

    ∇ r←CltSetUser userid
     ⍝ Cover-function for call to LockServerConnect from a Client
     
      Connect CONNECTION          ⍝ Register the connection
      r←SetUser CONNECTION userid ⍝ Set the user id
    ∇

    ∇ r←CltStatus dummy
     ⍝ Get Status information
     
      r←'LOCKSGRANTED' 'TASKS' 'USERS' 'RESOURCES' 'HELDBY' 'QUEUES'
      r←r(⍎⍕r)
    ∇

    ∇ Connect cmd;task;conn
     ⍝ Connection Arrived
     
      conn←1↓⊃(cmd='.')⊂cmd
     
      CONNS,←⊂conn
      TASKS,←task←NEXTTASK
      NEXTTASK←10000|NEXTTASK+1
      USERS←USERS,0
     
      0 Log'New connection ',conn,' assigned task id ',⍕task
    ∇

    ∇ Disconnect obj;m;i;held;task;conn
     ⍝ Connection Lost
     
      conn←1↓⊃(obj='.')⊂obj
      0 Log'Connection ',conn,' disconnected'
     
      :If (⍴m)≥i←(m←~CONNS∊⊂conn)⍳0
          task←i⊃TASKS
          :If 0≠⍴held←(HELDBY=task)/RESOURCES
              Release¨↓(⊂obj),[1.5]held ⍝ Release all held resources
          :EndIf
     
          QUEUES←{(⍵[;1]∊task)⌿⍵}¨QUEUES ⍝ Remove task from queues
          CONNS←m/CONNS
          TASKS←m/TASKS
          USERS←m/USERS
      :EndIf
    ∇

    ∇ {r}←InitLocks dummy
     ⍝ Intialise the Locks Daemon
     
      r←⍬             ⍝ Need a result
     
      LOCKSGRANTED←0  ⍝ Counter
     
      NEXTTASK←1      ⍝ Next Task ID
      CONNS←⍬         ⍝ TCP Sockets
      TASKS←⍬         ⍝ TASK IDs
      USERS←⍬         ⍝ USER IDs
      RESOURCES←⍬     ⍝ List of resources managed
      HELDBY←⍬        ⍝ TASK ID holding
      QUEUES←0⍴⊂0 3⍴0 ⍝ Queue for each resource (TASK, CONN, ARRIVAL TIME)
     
      LOGLEVEL←1      ⍝ 0=everything, 1=warnings, 2=errors
      MOCK←0          ⍝ Mockup
    ∇

    ∇ queue←Lock(cmd Resource);i;task;Conn
     ⍝ Returns queue length
     
      Conn←1↓⊃(cmd='.')⊂cmd
      task←(CONNS⍳⊂Conn)⊃TASKS
      queue←0
     
      :If (⍴RESOURCES)<i←RESOURCES⍳⊂Resource
          RESOURCES,←⊂Resource ⍝ Not currently in the table
          HELDBY,←task
          QUEUES,←⊂0 3⍴0
          Notify cmd Resource 0
      :ElseIf HELDBY[i]=0      ⍝ In the table but not held
          HELDBY[i]←task
          Notify cmd Resource 0
      :Else ⍝ It is already held
          (i⊃QUEUES)⍪←task cmd(3⊃⎕AI)
          queue←⊃⍴i⊃QUEUES
          :If LOGLEVEL=0
              0 Log'Resource ',Resource,' queued for ',(⍕task),' queue length=',⍕queue
          :EndIf
     
      :EndIf
    ∇

    ∇ level Log message
     
      →(level<LOGLEVEL)⍴0
      ⎕←(,'ZI2,<:>,ZI2,<:>,ZI2,<.>,ZI3'⎕FMT 1 4⍴3↓⎕TS),' ',message
    ∇

    ∇ MockTest;assert;START;resources;nprocesses;nresources;nevents;i;conns;conn;z;s
     
      assert←{'Assertion failed'⎕SIGNAL(⍵=0)/11}
     
      InitLocks 0
      LOGLEVEL←3 ⍝ Log everything
      MOCK←1
     
      Connect'C1'
      assert(1 0)≡TASKS,USERS
      SetUser'C1' 1234
      assert(1 1234)≡TASKS,USERS
     
      Connect'C2'
      SetUser'C2' 4321
     
      assert 0=Lock'C1' '/ALLOC10' ⍝ Granted
      assert HELDBY≡,1             ⍝ Held by Task 1
      Release'C1' '/ALLOC10'       ⍝ Release
      assert HELDBY≡,0             ⍝ Should now be free
     
      assert 0=Lock'C1' '/ALLOC10' ⍝ Granted
      assert HELDBY≡,1             ⍝ Held by Task 1
      assert 1=Lock'C2' '/ALLOC10' ⍝ Queued
      assert(2 'C2')≡2⍴⊃QUEUES     ⍝ Task 2 is in the queue
     
      Release'C1' '/ALLOC10'
      assert HELDBY≡,2             ⍝ Should now be held by Task 2
      assert 0=⊃⍴⊃QUEUES
     
      Disconnect'C2'
      assert 1=⍴TASKS
      assert HELDBY≡,0             ⍝ Should now be free
      Disconnect'C1'
      assert 0=⍴TASKS
     
     ⍝ --- performance test ---
     
      LOGLEVEL←3 ⍝ Erors only
     
      nprocesses←10
      nevents←1000×2×nprocesses
      ⎕←'Testing performance...'
      Connect¨conns←'C'∘,¨⍕¨⍳nprocesses
      SetUser¨↓conns,[1.5]⍳nprocesses
      resources←nevents⍴('/BLAH/BLAH/ALLOC'∘,¨⍕¨⍳nprocesses),nprocesses⍴⊂'/BLAH/BLAH/ALLOC0'
     
      START←3⊃⎕AI
      :For i :In ⍳nprocesses+nevents
          conn←(1+nprocesses|i-1)⊃conns
          :If i≤nevents ⋄ z←Lock conn(i⊃resources) ⋄ :EndIf
          :If i>nprocesses ⋄ z←Release conn((i-nprocesses)⊃resources) ⋄ :EndIf
      :EndFor
     
      s←0.001×(3⊃⎕AI)-START
      ⎕←(⍕nevents),' released & locked in',(1⍕s),'s (',(,' '~⍨,'CI12'⎕FMT nevents÷s),' locks/s)'
    ∇

    ∇ Notify(cmd Resource info);Conn;task
     ⍝ Notify connection that resource has been granted
     
      LOCKSGRANTED+←1
      :If LOGLEVEL=0
          Conn←1↓⊃(cmd='.')⊂cmd
          task←(CONNS⍳⊂Conn)⊃TASKS
          0 Log'Lock for ',Resource,' granted to task ',task
      :EndIf
     
      :If ~MOCK
          :If 0≠⊃r←#.DRC.Respond cmd(0(Resource info))
              1 Log'Respond to ',cmd,' failed'
          :EndIf
      :EndIf
    ∇

    ∇ Process(obj data);r;CONNECTION
     ⍝ Process a call. data[1] contains function name, data[2] an argument
     
     ⍝ {}##.DRC.Progress obj('    Thread ',(⍕⎕TID),' started to run: ',,⍕data) ⍝ Send progress report
      CONNECTION←obj
     
      :Trap 9999 ⋄ r←0((⍎1⊃data)(2⊃data))
      :Else ⋄ r←⎕EN ⎕DM
      :EndTrap
     
      :If 'CltLock'≢1⊃data ⍝ CltLock response will be sent by "Notify"
          {}##.DRC.Respond obj r
      :EndIf
    ∇

    ∇ {r}←Release(cmd Resource);i;conn;queue;start;task;Conn
     ⍝ Returns queue length
      Conn←1↓⊃(cmd='.')⊂cmd
     
      :If LOGLEVEL=0
          task←(CONNS⍳⊂Conn)⊃TASKS
          0 Log'Resource ',Resource,' released by task ',⍕task
      :EndIf
     
      :If (⍴RESOURCES)<i←RESOURCES⍳⊂Resource
          3 Log'Release of not-locked resource ',Resource,' by connection ',C1
          r←¯1
          →0
      :ElseIf 0=r←⊃⍴queue←i⊃QUEUES ⍝ No queue
          HELDBY[i]←r←0
      :Else ⍝ There is a queue
          (task conn start)←3⍴queue
          (i⊃QUEUES)←1 0↓queue  ⍝ Remove from queue
          HELDBY[i]←task
          Notify conn Resource(⎕AI[3]-start) ⍝ Notify of success
          r←⊃⍴queue
      :EndIf
    ∇

    ∇ r←Run(name port);sink;data;event;obj;rc;wait;z;cmd
     ⍝ Run the Lock Server - based on CONGA RPCServer sample
     
      :If 0=⎕NC'start' ⋄ start←1 ⋄ :EndIf
      {}##.DRC.Init''
     
      0 Log'Thread ',(⍕⎕TID),' is now handing server ''',name,'''.'
      done←0 ⍝ done←1 in function "End"
      :While ~done
          rc obj event data←4↑wait←##.DRC.Wait name 3000 ⍝ Time out now and again
     
          :Select rc
          :Case 0
              :Select event
              :Case 'Error'
                  :If 1119≢data ⋄ 3 Log'Error ',(⍕data),' on ',obj ⋄ :EndIf
                  :If ~done∨←name≡obj ⍝ Error on the listener itself?
                      {}##.DRC.Close obj ⍝ Close connection in error
                      Disconnect obj ⍝ Let logic know
                  :EndIf
     
              :Case 'Receive'
                  :If 2≠⍴data ⍝ Command is expected to be (function name)(argument)
                      {}##.DRC.Respond obj(99999 'Bad command format') ⋄ :Leave
                  :EndIf
     
                  :If ('Clt'≢3↑cmd)∨3≠⎕NC cmd←1⊃data ⍝ Command is expected to be a function in this ws
                      {}##.DRC.Respond obj(99999('Illegal command: ',cmd)) ⋄ :Leave
                  :EndIf
     
                  Process obj data ⍝ NB Single-threaded
     
              :Case 'Connect' ⍝ Ignored
     
              :Else ⍝ Unexpected result?
                  ∘
              :EndSelect
     
          :Case 100  ⍝ Time out - Insert code for housekeeping tasks here (deadlocks?)
     
          :Case 1010 ⍝ Object Not Found
              3 Log'Object ''',name,''' has been closed - RPC Server shutting down' ⋄ done←1
     
          :Else
              3 Log'Error in RPC.Wait: ',⍕wait
          :EndSelect
      :EndWhile
      ⎕DL 1 ⍝ Give responses time to complete
      {}##.DRC.Close name
      0 Log'Server ',name,' terminated.'
    ∇

    ∇ task←SetUser(cmd User);i;Conn
      ⍝ Return task ID
     
      Conn←1↓⊃(cmd='.')⊂cmd
     
      :If (⍴CONNS)<i←CONNS⍳⊂Conn
          3 Log'SetUser ',(⍕User),' for unknown connection ',Conn
      :Else
          0 Log'User set to ',(⍕User),' on connection ',Conn
          (i⊃USERS)←User
      :EndIf
     
      task←i⊃TASKS
    ∇

    ∇ {r}←Start port;sink;data;event;obj;rc;wait;z;cmd;name
     ⍝ Run the Lock Server - based on CONGA RPCServer sample
     
      {}##.DRC.Init''
      InitLocks 0
      {}##.DRC.Close name←'LOCKSRV'
     
      :If 0=1⊃r←##.DRC.Srv name''port'Command'
          1 Log'Server ''',name,''', listening on port ',⍕port
          2 Log'Handler thread started: ',⍕Run&name port
      :Else
          3 Log'Server failed to start: ',,⍕r
      :EndIf
    ∇

    ∇ Test;assert;START;resources;nprocesses;nresources;nevents;i;conns;conn;z;s
     
      assert←{'Assertion failed'⎕SIGNAL(⍵=0)/11}
     
      InitLocks 0
      LOGLEVEL←3 ⍝ Log everything
      MOCK←1     ⍝ Do not send CONGA messages
     
      Connect'.C1'
      assert(1 0)≡TASKS,USERS
      {}SetUser'.C1' 1234
      assert(1 1234)≡TASKS,USERS
     
      Connect'.C2'
      {}SetUser'.C2' 4321
     
      assert 0=Lock'.C1' '/ALLOC10' ⍝ Granted
      assert HELDBY≡,1             ⍝ Held by Task 1
      Release'.C1' '/ALLOC10'       ⍝ Release
      assert HELDBY≡,0             ⍝ Should now be free
     
      assert 0=Lock'.C1' '/ALLOC10' ⍝ Granted
      assert HELDBY≡,1             ⍝ Held by Task 1
      assert 1=Lock'.C2' '/ALLOC10' ⍝ Queued
      assert(2 '.C2')≡2⍴⊃QUEUES     ⍝ Task 2 is in the queue
     
      Release'.C1' '/ALLOC10'
      assert HELDBY≡,2             ⍝ Should now be held by Task 2
      assert 0=⊃⍴⊃QUEUES
     
      Disconnect'.C2'
      assert 1=⍴TASKS
      assert HELDBY≡,0             ⍝ Should now be free
      Disconnect'.C1'
      assert 0=⍴TASKS
     
     ⍝ --- performance test ---
     
      LOGLEVEL←3 ⍝ Erors only
     
      nprocesses←10
      nevents←1000×2×nprocesses
      ⎕←'Testing performance...'
      Connect¨conns←'.C'∘,¨⍕¨⍳nprocesses
      {}SetUser¨↓conns,[1.5]⍳nprocesses
      resources←nevents⍴('/BLAH/BLAH/ALLOC'∘,¨⍕¨⍳nprocesses),nprocesses⍴⊂'/BLAH/BLAH/ALLOC0'
     
      START←3⊃⎕AI
      :For i :In ⍳nprocesses+nevents
          conn←(1+nprocesses|i-1)⊃conns
          :If i≤nevents ⋄ z←Lock conn(i⊃resources) ⋄ :EndIf
          :If i>nprocesses ⋄ z←Release conn((i-nprocesses)⊃resources) ⋄ :EndIf
      :EndFor
     
      s←0.001×(3⊃⎕AI)-START
      ⎕←(⍕nevents),' released & locked in',(1⍕s),'s (',(,' '~⍨,'CI12'⎕FMT nevents÷s),' locks/s)'
    ∇

    ∇ TestClient user;mine;nevents;START;i;s;z;clt
     
      assert←{'Assertion failed'⎕SIGNAL(⍵=0)/11}
     
      clt←## ⍝ Location of Client functions
     
      ⎕←'Logged in as task #',⍕clt.LockServerInit'127.0.0.1' 8888 user
     
      nevents←1000
      ⎕←'Testing performance...'
     
      START←3⊃⎕AI
      :For i :In ⍳nevents
          z←clt.∆ENQ'/PORTFOLIO'user
          z←clt.∆CLS'/PORTFOLIO'user
      :EndFor
     
      s←0.001×(3⊃⎕AI)-START
      ⎕←(⍕nevents),' released & locked in',(1⍕s),'s (',(,' '~⍨,'CI12'⎕FMT nevents÷s),' locks/s)'
     
      #.DRC.Close #.LOCKSERVER
      ⎕EX'#.LOCKSERVER'
    ∇

    assert←{'Assertion failed'⎕SIGNAL(⍵=0)/11}

⍝ ** Those ops cannot be recreated: clt

:EndNamespace
