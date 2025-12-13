package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

/* ===================== STRUCTS ===================== */

type StorageNode struct {
	ID  string  `json:"id"`
	URL string  `json:"url"`
	Lat float64 `json:"lat"`
	Lon float64 `json:"lon"`
}

type FileMeta struct {
	ID         string   `json:"id"`
	Filename   string   `json:"filename"`
	Size       int64    `json:"size"`
	Replicated []string `json:"replicated"`
	CreatedAt  string   `json:"created_at"`
}

/* ===================== GLOBALS ===================== */

var (
	nodes  []StorageNode
	files  = map[string]FileMeta{}
	mu     sync.Mutex
	dbFile = "files.json.tmp"
	client = &http.Client{Timeout: 20 * time.Second}
)

/* ===================== UTILS ===================== */

func loadDB() {
	if b, err := os.ReadFile(dbFile); err == nil {
		_ = json.Unmarshal(b, &files)
	}
}

func saveDB() {
	b, _ := json.MarshalIndent(files, "", "  ")
	_ = os.WriteFile(dbFile, b, 0644)
}

func haversine(aLat, aLon, bLat, bLon float64) float64 {
	const R = 6371
	dLat := (bLat - aLat) * math.Pi / 180
	dLon := (bLon - aLon) * math.Pi / 180
	la1 := aLat * math.Pi / 180
	la2 := bLat * math.Pi / 180
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Sin(dLon/2)*math.Sin(dLon/2)*math.Cos(la1)*math.Cos(la2)
	return 2 * R * math.Asin(math.Sqrt(h))
}

/* ===================== MAIN ===================== */

func main() {
	port := flag.Int("port", 8000, "central port")
	nodesFile := flag.String("nodes", "./files.json", "nodes config")
	flag.Parse()

	// Load nodes
	raw, err := os.ReadFile(*nodesFile)
if err != nil {
    log.Fatal("FAILED TO READ files.json:", err)
}

err = json.Unmarshal(raw, &nodes)
if err != nil {
    log.Fatal("FAILED TO PARSE files.json:", err)
}

log.Println("NODES LOADED:", len(nodes))
for _, n := range nodes {
    log.Println("NODE:", n.ID, n.URL)
}


	// Serve UI
	http.Handle("/", http.FileServer(http.Dir("./")))

	/* ---------- LIST FILES ---------- */
	http.HandleFunc("/list", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(files)
	})

	/* ---------- UPLOAD ---------- */
	http.HandleFunc("/upload", func(w http.ResponseWriter, r *http.Request) {
	log.Println("UPLOAD HIT")

	r.ParseMultipartForm(200 << 20)

	f, fh, err := r.FormFile("file")
	if err != nil {
		log.Println("UPLOAD ERROR: no file")
		http.Error(w, "file required", 400)
		return
	}
	defer f.Close()

	log.Println("UPLOAD FILE:", fh.Filename)

	data, _ := io.ReadAll(f)
	id := fmt.Sprintf("%d", time.Now().UnixNano())

	rep := []string{}

	for _, n := range nodes {
		log.Println("REPLICATING TO:", n.ID)

		var buf bytes.Buffer
		wr := multipart.NewWriter(&buf)
		wr.WriteField("id", id)
		wr.WriteField("filename", fh.Filename)

		part, _ := wr.CreateFormFile("file", fh.Filename)
		part.Write(data)
		wr.Close()

		req, _ := http.NewRequest("POST", n.URL+"/replicate", &buf)
		req.Header.Set("Content-Type", wr.FormDataContentType())

		resp, err := client.Do(req)
		if err != nil {
			log.Println("REPLICATE FAILED:", n.ID, err)
			continue
		}

		log.Println("REPLICATE STATUS:", n.ID, resp.Status)
		resp.Body.Close()

		if resp.StatusCode == 201 {
			rep = append(rep, n.ID)
		}
	}

	mu.Lock()
	files[id] = FileMeta{
		ID: id, Filename: fh.Filename,
		Size: int64(len(data)), Replicated: rep,
	}
	saveDB()
	mu.Unlock()

	log.Println("UPLOAD DONE:", id)

	w.WriteHeader(http.StatusCreated)
})


	/* ---------- NEAREST ---------- */
	http.HandleFunc("/nearest/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/nearest/")

		if len(nodes) == 0 {
			http.Error(w, "no storage nodes configured", http.StatusServiceUnavailable)
			return
		}

		mu.Lock()
		meta, ok := files[id]
		mu.Unlock()
		if !ok {
			http.NotFound(w, r)
			return
		}

		if len(meta.Replicated) == 0 {
			http.Error(w, "file not replicated yet", http.StatusServiceUnavailable)
			return
		}

		// Default SEA → Singapore
		lat := 1.3521
		lon := 103.8198

		best := nodes[0]
		bestD := 1e18

		for _, n := range nodes {
			for _, rid := range meta.Replicated {
				if n.ID == rid {
					d := haversine(lat, lon, n.Lat, n.Lon)
					if d < bestD {
						bestD = d
						best = n
					}
				}
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(best)
	})

	/* ---------- FILE (REDIRECT) ---------- */
	http.HandleFunc("/file/", func(w http.ResponseWriter, r *http.Request) {
		if len(nodes) == 0 {
			http.Error(w, "no storage nodes configured", http.StatusServiceUnavailable)
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/file/")
		http.Redirect(w, r, nodes[0].URL+"/file/"+id, http.StatusTemporaryRedirect)
	})

	/* ---------- DELETE ---------- */
	http.HandleFunc("/delete/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/delete/")
		for _, n := range nodes {
			req, _ := http.NewRequest("DELETE", n.URL+"/delete/"+id, nil)
			client.Do(req)
		}
		mu.Lock()
		delete(files, id)
		saveDB()
		mu.Unlock()
		w.Write([]byte("deleted"))
	})

	log.Println("CENTRAL running on :", *port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}
