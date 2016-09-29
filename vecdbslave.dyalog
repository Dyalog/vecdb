:Namespace vecdbslave

    (⎕IO ⎕ML)←1 1
    LOGLEVEL←0

    fmtts←{,'ZI4,<->,ZI2,<->,ZI2,< >,ZI2,<:>,ZI2,<:>,ZI2' ⎕FMT 1 6⍴⍵}
    
    ∇ {r}←Shutdown dummy
     ⍝ Shut down slave
      
      DB.Close        ⍝ Close the vecdb
      ⎕EX 'DB'
      done←1          ⍝ Global flag to shut down
      r←⍬             ⍝ Need a result
    ∇

    ∇ Init(folder shards)
      STATE←1 ⍝ Starting, 0=Running, 2=Startup Failed, 3=Shut Down
      1 Log STATUS←'Startup initiated at ',fmtts ⎕TS
      CONNS←TASKS←USERS←TOKENS←⍬
      NEXTTASK←1000
     
      :Trap 0
          DB←⎕NEW ##.vecdb(folder shards)
          STATE←0
          1 Log'Slave startup completed, ',STATUS←'Folder= ',folder,', shards= ',⍕shards
      :Else  
          STATE←2 ⍝ Startup Failed
          3 Log STATUS←'Startup failed: ',∊⎕DM
          ∘∘∘
      :EndTrap
    ∇

    ∇ {r}←Start(folder shards port);sink;data;event;obj;rc;wait;z;cmd;name
     ⍝ Run a vecdb Slave - based on CONGA RPCServer sample
     
      {}##.DRC.Init''
      {}##.DRC.Close name←'VECSRV'
     
      Init folder shards
     
      :If 0=1⊃r←##.DRC.Srv name''port'Command'
          1 Log'Server ''',name,''', listening on port ',⍕port
          2 Log'Handler thread started: ',⍕Run&name port
      :Else
          3 Log'Server failed to start: ',,⍕r
      :EndIf
    ∇

    ∇ Connect cmd;task;conn
     ⍝ Connection Created
     
      conn←1↓⊃(cmd='.')⊂cmd
      CONNS,←⊂conn
      TASKS,←task←NEXTTASK
      NEXTTASK←10000|NEXTTASK+1
      USERS←USERS,0
      TOKENS←TOKENS,⊂''
     
      0 Log'New connection ',conn,' assigned task id ',⍕task
    ∇

    ∇ Disconnect obj;m;i;held;task;conn
     ⍝ Connection Lost
     
      conn←1↓⊃(obj='.')⊂obj
      0 Log'Connection ',conn,' disconnected'
     
      :If (⍴m)≥i←(m←~CONNS∊⊂conn)⍳0
          CONNS←m/CONNS
          TASKS←m/TASKS
          USERS←m/USERS
          TOKENS←m/TOKENS
      :EndIf
    ∇

    ∇ level Log message     
      →(level<LOGLEVEL)⍴0
      ⎕←(,'ZI2,<:>,ZI2,<:>,ZI2,<.>,ZI3'⎕FMT 1 4⍴3↓⎕TS),' ',message
    ∇

    ∇ Process(obj data);r;CONNECTION;cmd;arg;close;txt
     ⍝ Process a call. data[1] contains function name, data[2] an argument
     
     ⍝ {}##.DRC.Progress obj('    Thread ',(⍕⎕TID),' started to run: ',,⍕data) ⍝ Send progress report
      CONNECTION←obj
      Conn←1↓⊃(obj='.')⊂obj
      (cmd arg)←2↑data
      close←0

      :If (⊂cmd)∊'SetToken' 'SetUser' 'Shutdown'
          r←0 (⍎cmd,' obj arg')
     
      :ElseIf (⊂cmd)∊'Append' 'Count' 'Query' 'Update' 'Read' 
          :If 0≠≢(CONNS⍳⊂Conn)⊃TOKENS,⊂''
              :Trap 9999 
                 :If cmd≡'Count' ⋄ r←0 DB.Count 
                 :Else ⋄ r←0 ((DB⍎cmd) arg)
                 :EndIf
              :Else ⋄ r←⎕EN ⎕DM
              :EndTrap     
          :Else
              close←1
              r←999 ('No valid token provided for command ',⍕cmd arg)
          :EndIf     

      :Else
          r←999 ('Unsupported command: ',cmd)
      :EndIf
     
      {}##.DRC.Respond obj r

      :If close
          ⍝ /// {{}##.DRC.Close ⍵⊣⎕DL 1}&Conn ⍝ Start thread which waits 1s then closes
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
     
                  Process obj data ⍝ NB Single-threaded
     
              :Case 'Connect'
                  Connect obj

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
      
      :If 2=⎕NC '#.AUTOSHUT'
      :AndIf 0≠#.AUTOSHUT
          ⎕OFF
      :EndIf
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

    ∇ task←SetToken(cmd Token);i;Conn
      ⍝ Return task ID
     
      Conn←1↓⊃(cmd='.')⊂cmd
     
      :If (⍴CONNS)<i←CONNS⍳⊂Conn
          3 Log'SetToken ',(⍕Token),' for unknown connection ',Conn
      :Else
          0 Log'Token set to ',(⍕Token),' on connection ',Conn
          (i⊃TOKENS)←Token
      :EndIf
     
      task←i⊃TASKS
    ∇

:EndNamespace
