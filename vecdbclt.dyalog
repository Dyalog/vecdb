:Namespace vecdbclt

    (⎕IO ⎕ML)←1 1
    SERVER←''

    ∇ r←Clt(connection address port)
      :If 1111=⊃r←##.DRC.Clt connection address port
          {}⎕DL 0.5
      :AndIf 1111=⊃r←##.DRC.Clt connection address port
          {}⎕DL 1
      :AndIf 1111=⊃r←##.DRC.Clt connection address port
          {}⎕DL 3
      :AndIf 1111=⊃r←##.DRC.Clt connection address port
          {}⎕DL 5
      :AndIf 1111=⊃r←##.DRC.Clt connection address port
          (⍕r)⎕SIGNAL 11
      :EndIf
    ∇

    ∇ {r}←{connection}Connect(address port user)
     ⍝ Connect to vecdb server process
     
      :If 0=⎕NC'connection' ⋄ connection←'VECDB' ⋄ :EndIf
     
      :If 0=⊃r←##.DRC.Init''
          :If 0≠⍴connection ⋄ {}##.DRC.Close connection ⋄ :EndIf
      :AndIf 0=⊃r←Clt connection address port
          CONNECTION←2⊃r
      :Else
          ('Error: ',,⍕r)⎕SIGNAL 11
      :EndIf
    ∇

    ∇ r←SrvDo(client cmd)
      ⍝ Send a command to vecdb and await the result
     
      r←SrvRcv SrvSend client cmd
    ∇

    ∇ cmd←SrvSend(client cmd);r
    ⍝ Return command name to wait on
      :If 0=⊃r←##.DRC.Send client cmd
          cmd←2⊃r
      :Else
          (⍕r)⎕SIGNAL 11
      :EndIf
    ∇

    ∇ r←SrvRcv c;done;wr;z
    ⍝ Wait for result from vecdb, signal DOMAIN ERROR if it fails
     
      :Repeat
          :If ~done←∧/100 0≠1⊃r←##.DRC.Wait c 10000 ⍝ Only wait 10 seconds
     
              :Select 3⊃r
              :Case 'Error'
                  done←1
              :Case 'Progress'
                  ⎕←'Progress: ',4⊃r
              :Case 'Receive'
                  :If 0=⊃r
                      r←4⊃r
                  :AndIf 0=⊃r
                      r←2⊃r
                      done←1
                  :Else
                      ('Error: ',,⍕r)⎕SIGNAL 11
                  :EndIf
              :EndSelect
          :EndIf
      :Until done
    ∇

    ∇ r←Open folder
     ⍝ Cover-function for call to Lock from a Client
     
      r←⎕NEW vecdbproxy(folder CONNECTION)
    ∇

    :Class vecdbproxy
    ⍝ Produce a vecdb proxy object for a served vecdb

        ∇ Open(folder connection)
          :Access Public
          :Implements Constructor
          (FOLDER CONNECTION)←folder connection
          :If 0=⊃r←##.SrvDo CONNECTION('Open'folder)
              ⎕DF'[vecdbclt: ',folder,']'
          :Else
              (⍕r)⎕SIGNAL 11
          :EndIf
        ∇

        ∇ {r}←Shutdown msg
          :Access Public
          :If 0=⊃r←##.SrvDo CONNECTION('Shutdown'msg)
              {}#.DRC.Close CONNECTION
              CONNECTION←''
          :EndIf
        ∇

        ∇ Close
          :Access Public
          :If 0=⊃r←##.SrvDo CONNECTION('Close'⍬)
              {}#.DRC.Close CONNECTION
              CONNECTION←''
          :EndIf
        ∇

        ∇ r←Count
          :Access Public
          :If 0≠⍴CONNECTION
              r←##.SrvDo CONNECTION('Count'(FOLDER ⍬))
              r←+/r
          :Else
              'CONNECTION CLOSED'⎕SIGNAL 11
          :EndIf
        ∇

        ∇ r←Append args
          :Access Public
          :If 0≠⍴CONNECTION
              r←##.SrvDo CONNECTION('Append'(FOLDER args))
          :Else
              'CONNECTION CLOSED'⎕SIGNAL 11
          :EndIf
        ∇

        ∇ r←Query args
          :Access Public
          :If 0≠⍴CONNECTION
              r←##.SrvDo CONNECTION('Query'(FOLDER args))
              :If 2=⍴⍴⊃r
                  r←⊃⍪/r
              :Else
                  r←⊃,¨/r
              :EndIf
          :Else
              'CONNECTION CLOSED'⎕SIGNAL 11
          :EndIf
        ∇

        ∇ r←Read args
          :Access Public
          :If 0≠⍴CONNECTION
              r←##.SrvDo CONNECTION('Read'(FOLDER args))
              r←⊃,¨/r
          :Else
              'CONNECTION CLOSED'⎕SIGNAL 11
          :EndIf
        ∇

        ∇ r←Update args
          :Access Public
          :If 0≠⍴CONNECTION
              r←##.SrvDo CONNECTION('Update'(FOLDER args))
          :Else
              'CONNECTION CLOSED'⎕SIGNAL 11
          :EndIf
         
        ∇

    :EndClass

:EndNamespace
