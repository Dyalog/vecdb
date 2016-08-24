:Namespace vecdbclt

    (⎕IO ⎕ML ⎕WX)←1 0 3
    SERVER←''

    ∇ r←Connect(address port user)
     ⍝ Connect to vecdb server process
     
      :If 0=⊃r←##.DRC.Init''
          {}##.DRC.Close CONNECTION←'VECDB'
      :AndIf 0=⊃r←##.DRC.Clt CONNECTION address port
      :AndIf 0=⊃r←SrvDo CONNECTION('CltSetUser'user)
          r←SrvDo CONNECTION('CltSetUser'user)
      :Else
          ('Error: ',,⍕r)⎕SIGNAL 11
      :EndIf
    ∇

    ∇ r←SrvDo(client cmd);c;done;wr;z
⍝ Send a command to vecdb, signal DOMAIN ERROR if it fails
     
      :If 0=1⊃r c←2↑##.DRC.Send client cmd
          :Repeat
              :If ~done←∧/100 0≠1⊃r←##.DRC.Wait c 10000 ⍝ Only wait 10 seconds
     
                  :Select 3⊃r
                  :Case 'Error'
                      done←1
                  :Case 'Progress'
                 ⍝ progress report - update your GUI with 4⊃r?
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
      :EndIf
    ∇

    ∇ r←Open database
     ⍝ Cover-function for call to Lock from a Client
     
      :If 0=⊃r←SrvDo CONNECTION ('CltOpen' database) 
          r←⎕NEW vecdbproxy (2⊃r)
      :Else
          (,⍕r) ⎕SIGNAL 11
      :EndIf
    ∇           
    
    :Class vecdbproxy
    ⍝ Produce a vecdb proxy object for a served vecdb
     
     ∇Open (name connection)
     :Access Public
     :Implements Constructor
     (NAME CONNECTION)←name connection

     ∇
     
     ∇Close
     :Access Public

     ∇
                                      
     ∇r←Append args
     :Access Public
∇                                

     ∇r←Query args
     :Access Public
     ∇
                                     
     ∇r←Read args
     :Access Public
     ∇        
                             
     ∇r←Update args
     ∇                                

    :EndClass

:EndNamespace
