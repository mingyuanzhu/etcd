package main

//func TestLoad(t *testing.T) {
//	client := &http.Client{Timeout: time.Second * 5}
//	url := "localhost:22380/key/"
//	wg := new(sync.WaitGroup)
//	wg.Add(10)
//	for i := 0; i < 1; i++ {
//		go func(index int) {
//			for j := 0; j < 1; j++ {
//				k := fmt.Sprintf("k_%d_%d", i, j)
//				v := fmt.Sprintf("v_%d_%d", i, j)
//
//				req, err := http.NewRequest("POST", url+k, bytes.NewBufferString(v))
//				req.Header.Set("Content-Type", "text/plain")
//				if err != nil {
//					fmt.Println("get request error")
//				}
//				resp, err := client.Do(req)
//				if err != nil {
//					panic(err)
//				}
//				fmt.Println(resp.Status)
//				resp.Body.Close()
//				wg.Done()
//			}
//		}(i)
//	}
//	wg.Wait()
//	fmt.Println("finish test")
//}
