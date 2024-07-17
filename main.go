package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"git.tcp.direct/kayos/common/entropy"
	"git.tcp.direct/kayos/common/pool"
	"git.tcp.direct/kayos/common/squish"
	"github.com/briandowns/spinner"
	"github.com/dsoprea/go-exif/v3"
	exifcommon "github.com/dsoprea/go-exif/v3/common"
	"github.com/go-audio/wav"
	"github.com/h2non/filetype"
	"github.com/panjf2000/ants/v2"
	"golang.org/x/text/encoding/unicode"

	"github.com/dhowden/tag"
)

const maxFilenameLength = 64

func printArt(s string) {
	up, _ := squish.UnpackStr(s)
	_, _ = os.Stdout.WriteString(up)
	time.Sleep(5 * time.Millisecond)
}

// ineff assignment, flags assign the worker counts
var workers, _ = ants.NewMultiPool(runtime.NumCPU(), 5, ants.RoundRobin)

type Image struct {
	Time int64
	Path string
}

var (
	maxNumberOfFilesPerFolder int
	splitMonths               bool
	source                    string
	destination               = new(string)
	keepFilename              bool
	dateTimeFilename          bool
	minEventDeltaDays         int
	fileBusy                  = make(map[string]*int64)
	busyIndexLock             sync.RWMutex
	verbose                   bool
	workingSpinner            *spinner.Spinner
	spinnerMu                 sync.RWMutex
	printMutex                sync.Mutex
	printQueue                = make(chan printfEntry, 10000)
)

func getSpinner() *spinner.Spinner {
	spinnerMu.RLock()
	s := workingSpinner
	spinnerMu.RUnlock()
	return s
}

func setSpinner(s *spinner.Spinner) {
	spinnerMu.Lock()
	workingSpinner = s
	spinnerMu.Unlock()
}

type printfEntry struct {
	format string
	a      []any
}

func newPrintfEntry(format string, a ...any) printfEntry {
	return printfEntry{format: format, a: a}
}

type lock int64

const (
	unlocked lock = 0
	locked   lock = 1
)

func printlnf(format string, a ...interface{}) {
	if !printMutex.TryLock() {
		printQueue <- newPrintfEntry(format, a...)
		return
	}

	do := func(ft string, aa ...interface{}) {
		if !strings.Contains(ft, "\n") {
			ft += "\n"
		}
		spinnerMu.Lock()
		if workingSpinner != nil && workingSpinner.Active() {
			workingSpinner.Stop()
			defer workingSpinner.Start()
		}
		fmt.Printf(ft, aa...)
		spinnerMu.Unlock()
	}

	do(format, a...)

backlog:
	for {
		select {
		case entry := <-printQueue:
			do(entry.format, entry.a...)
		default:
			break backlog
		}
	}

	printMutex.Unlock()
}

func fileIsBusy(path string) bool {
	busyIndexLock.RLock()
	_, ok := fileBusy[path]
	if !ok {
		busyIndexLock.RUnlock()
		return false
	}
	res := atomic.CompareAndSwapInt64(fileBusy[path], int64(locked), int64(locked))
	busyIndexLock.RUnlock()
	return res
}

func lockFile(path string) {
	busyIndexLock.RLock()
	_, ok := fileBusy[path]
	if !ok {
		busyIndexLock.RUnlock()
		busyIndexLock.Lock()
		fileBusy[path] = new(int64)
		atomic.StoreInt64(fileBusy[path], int64(locked))
		busyIndexLock.Unlock()
		return
	}
	busyIndexLock.RUnlock()
	// exponential back off on the spinlocc
	multiplier := 0
	for {
		busyIndexLock.RLock()
		if atomic.LoadInt64(fileBusy[path]) == int64(locked) {
			busyIndexLock.RUnlock()
			break
		}

		if atomic.CompareAndSwapInt64(fileBusy[path], int64(unlocked), int64(locked)) {
			busyIndexLock.RUnlock()
			break
		}
		busyIndexLock.RUnlock()
		multiplier++
		target := 100
		if multiplier > 10 {
			target = target * multiplier / 10
		}
		println("spinlock (" + path + "): sleeping for somewhere between 0 and " + strconv.Itoa(target) + " milliseconds...")
		entropy.RandSleepMS(target)
	}
}
func unlockFile(path string) {
	busyIndexLock.Lock()
	_, ok := fileBusy[path]
	if !ok {
		busyIndexLock.Unlock()
		return
	}
	atomic.StoreInt64(fileBusy[path], int64(unlocked))
	busyIndexLock.Unlock()
}

const (
	colPre = "\033["
	colSuf = "m"
	red    = colPre + "31" + colSuf
	green  = colPre + "32" + colSuf
	blue   = colPre + "36" + colSuf
	reset  = colPre + "0" + colSuf
)

func getCameraInfo(rawExif []byte) (map[string]string, bool) {
	if len(rawExif) == 0 {
		return nil, false
	}

	et, err := getExifMap(rawExif)
	if err != nil {
		return nil, false
	}

	for k, v := range et {
		println("TODO: determine camera related tags, dumping...")
		println(k)
		for _, vv := range v {
			if vv == nil {
				continue
			}
			printlnf("\t(%T): %v\n", vv, cleanString(fmt.Sprintf("%v", vv)))
		}
	}

	return nil, false
}

func getExifMap(rawExif []byte) (exifTags, error) {
	if len(rawExif) == 0 {
		return nil, errors.New("empty exif")
	}

	if existing, ok := exifCache.get(rawExif); ok {
		return existing, nil
	}

	im, err := exifcommon.NewIfdMappingWithStandard()
	if err != nil {
		printlnf(red+"Could not create IFD mapping: "+reset+"%v\n", err)
		return nil, err
	}

	ti := exif.NewTagIndex()
	et := newExifTags()

	var creationTime *time.Time

	visitor := func(ite *exif.IfdTagEntry) (err error) {
		defer func() {
			if state := recover(); state != nil {
				printlnf(red+"panic recovered, state: "+reset+"%v\n", state)
				return
			}
		}()

		val, err := ite.Value()
		if err == nil && val != nil && ite.TagName() != "" {
			if _, ok := et[ite.TagName()]; !ok {
				et[ite.TagName()] = make([]any, 0)
			}
			et[ite.TagName()] = append(et[ite.TagName()], val)
			// if verbose {
			if len(fmt.Sprintf("%v", val)) > 0 {
				printlnf(
					"\t"+blue+ite.TagName()+": "+reset+
						fmt.Sprintf("(%T) ", val), cleanString(fmt.Sprintf("%v", val)),
				)
			}
			// }
		}

		tagId := ite.TagId()
		tagType := ite.TagType()

		switch {
		case (tagType == exifcommon.TypeAscii && (tagId == 0x9003 || tagId == 0x9004 || tagId == 0x0132)) ||
			ite.TagName() == "DateTimeOriginal":
			if v, e := ite.FormatFirst(); e == nil {
				if t, te := exifcommon.ParseExifFullTimestamp(v); te == nil {
					if t.IsZero() {
						return nil
					}
					if creationTime == nil || t.Before(*creationTime) {
						creationTime = &t
					}
				}
			}
		default:
			return nil
		}
		return nil
	}

	if _, _, err = exif.Visit(exifcommon.IfdStandardIfdIdentity, im, ti, rawExif, visitor, nil); err != nil {
		// printlnf(red+"Could not extract tags: "+reset+"%v\n", err))
		return nil, err
	}

	if creationTime != nil {
		et["creationtime"] = []any{creationTime}
	}

	exifCache.put(rawExif, et)

	return et, nil
}

func getMinimumCreationTime(rawExif []byte) (*time.Time, error) {
	var (
		creationTime *time.Time
	)

	exifMap, err := getExifMap(rawExif)

	if ct, ok := exifMap["creationtime"]; ok && len(ct) > 0 {
		creationTime = ct[0].(*time.Time)
	}

	if creationTime == nil {
		err = fmt.Errorf("no creation time found")
	}

	return creationTime, err
}

var bufs = pool.NewBufferFactory()

const kilo = 1024 * 1024

func getRawExif(imagePath string) ([]byte, error) {
	// printlnf("Getting raw exif for " + imagePath)
	/*	lockFile(imagePath)
		defer unlockFile(imagePath)*/

	buf := bufs.Get()
	// 64KB
	_ = buf.Grow(kilo * 128)

	f, err := os.Open(imagePath)
	if err != nil {
		// printlnf(red+"Could not open file %s: "+reset+"%v\n", imagePath, err))
		return nil, err
	}

	n, err := buf.ReadFrom(io.LimitReader(f, kilo*256))

	if err != nil || n == 0 {
		if err == nil {
			err = fmt.Errorf("no data read")
		}
		// printlnf(red+"Could not read file %s: "+reset+"%v\n", imagePath, err))
		return nil, err
	}

	rawExif, err := exif.SearchAndExtractExifWithReader(buf)
	bufs.MustPut(buf)
	if len(rawExif) == 0 && err == nil {
		err = fmt.Errorf("no exif data found")
	}
	return rawExif, err
}

func postprocessImage(imagePath string, destinationRoot string, wg *sync.WaitGroup) {
	defer wg.Done()

	rawExif, err := getRawExif(imagePath)

	if err != nil {
		// printlnf("Invalid EXIF data for %s: %s\n", imagePath, err.Error()))
		return
	}

	creationTime, err := getMinimumCreationTime(rawExif)
	if creationTime == nil || err != nil {
		//		fileInfo, err := os.Stat(imagePath)
		//		if err != nil {
		if verbose {
			printlnf("Could not get file info for %s: %v\n", imagePath, err)
		}
		return
		//		}
		//		creationT := fileInfo.ModTime()
		//		creationTime = &creationT
	}

	printlnf("Creation time found for %s: %s\n", imagePath, creationTime.Format(time.RFC3339))

	_, _ = getCameraInfo(rawExif) // FIXME TODO

	if err = writeImage(Image{Time: creationTime.Unix(), Path: imagePath}, destinationRoot); err != nil {
		printlnf(red + err.Error() + reset)
	}
}

func createPath(newPath string) {
	newPath = strings.TrimPrefix(newPath, ".")
	newPath = filepath.Clean(newPath)
	if newPath == "" {
		return
	}
	if _, err := os.Stat(newPath); os.IsNotExist(err) {
		if err := os.MkdirAll(newPath, 0755); err != nil {
			panic(fmt.Errorf("%s: %w", newPath, err))
		}
	}
}

func writeImage(image Image, destinationRoot string) error {
	// minEventDelta := int64(minEventDeltaDays * 60 * 60 * 24)

	t := time.Unix(image.Time, 0)
	fileName := filepath.Base(image.Path)
	if t.IsZero() {
		return fmt.Errorf("image dosen't have a valid creation date: %s", image.Path)
	}

	destinationDir := ""
	destinationFilePath := ""

	thenYear, thenMonth, thenDay := t.Date()
	nowYear, nowMonth, nowDay := time.Now().Date()

	if thenYear == nowYear && thenMonth == nowMonth && thenDay == nowDay {
		return fmt.Errorf("image crated today: %s", image.Path)
	}

	destinationDir = filepath.Join(
		destinationRoot, strconv.Itoa(thenYear), thenMonth.String(), strconv.Itoa(thenDay),
	)
	if err := os.MkdirAll(destinationDir, 0755); err != nil {
		return fmt.Errorf("failed to make directory %s: %w", destinationDir, err)
	}

	destinationFilePath = filepath.Join(destinationDir, fileName)

	if destinationFilePath == "" {
		return fmt.Errorf("invalid destination path for %s", image.Path)
	}

	println("moving " + image.Path + " to " + destinationFilePath)
	if err := os.Rename(image.Path, destinationFilePath); err != nil {
		return fmt.Errorf("failed to move file %s -> %s : %w", image.Path, destinationFilePath, err)
	}

	return nil
}

func postprocessImages(imageDirectory string, destinationDirectory string) {
	var wg = &sync.WaitGroup{}
	wg.Add(1)

	setSpinner(spinner.New(spinner.CharSets[34], 50*time.Millisecond, spinner.WithColor("blue")))

	printlnf("\n\n" + blue + "processing images..." + reset)
	fmt.Print("\n")

	getSpinner().Start()

	if err := filepath.Walk(imageDirectory, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			wg.Add(1)
			_ = workers.Submit(func() { postprocessImage(path, destinationDirectory, wg) })
		}
		return nil
	}); err != nil {
		printlnf("Error walking the path %s: %v\n", imageDirectory, err)
		getSpinner().Stop()
		wg.Done()
		return
	} else {
		go func() {
			time.Sleep(1 * time.Second)
			wg.Done()
		}()
		wg.Wait()
	}

}

func log(logString string) {
	printlnf("\n\n%s: %s", time.Now().Format("15:04:05\n\n"), logString)
}

func fileExists(filename string) bool {
	_, err := os.Stat(filename)
	return !os.IsNotExist(err)
}

var errCt int64 = 0

func symLink(src, dst string) {

	if atomic.LoadInt64(&errCt) > 50 {
		printlnf("\n\n" + red + "Too many errors, exiting" + reset)
		fmt.Print("\n\n")
		os.Exit(1)
	}

	race := false

	if fileIsBusy(src) {
		println("race (src): " + src)
		race = true
	}

	if fileIsBusy(dst) {
		println("race (dst): " + src)
		race = true
	}

	if race {
		return
	}

	lockFile(dst)
	lockFile(src)
	defer func() {
		unlockFile(src)
		unlockFile(dst)
	}()

	handleErr := func() {
		atomic.AddInt64(&errCt, 1)
		time.Sleep(100 * time.Millisecond)
	}

	dstat, err := os.Stat(filepath.Dir(dst))
	switch {
	case err == nil && !dstat.IsDir():
		printlnf(red + "destination path (" + filepath.Dir(dst) + ") exists and isn't a directory!" + reset + "\n")
		handleErr()
		return
	case err == nil:
		break
	case os.IsNotExist(err):
		if err = os.MkdirAll(filepath.Dir(dst), 0755); err != nil {
			if os.IsExist(err) {
				break
			}
			printlnf(red + "failed to create directory!" + reset)
			handleErr()
			return
		}
	}

	if err = os.Symlink(src, dst); err == nil {
		return
	}

	switch {
	case strings.Contains(err.Error(), ": file exists"):
		if existingLink, rlerr := os.Readlink(dst); rlerr == nil {
			if existingLink == src {
				return
			}
		}
	default:
		printlnf(red+"Could not create symlink %s"+reset+": %v\n", dst, err)
		handleErr()
	}
}

func limitFilesPerFolder(folder string, maxNumberOfFilesPerFolder int) *sync.WaitGroup {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	if err := filepath.Walk(folder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if strings.Contains(path, "Processed_Images") {
			return nil
		}
		if !info.IsDir() {
			return nil
		}
		wg.Add(1)
		_ = workers.Submit(func() {
			defer wg.Done()
			filesInFolder, err := os.ReadDir(path)
			if err != nil {
				printlnf("%sCould not read directory %s%s: %v\n", red, path, err, reset)
				return
			}
			adjustedFilesInFolder := make([]os.DirEntry, 0)
			for _, de := range filesInFolder {
				if de.IsDir() {
					continue
				}
				adjustedFilesInFolder = append(adjustedFilesInFolder, de)
			}
			filesInFolder = adjustedFilesInFolder
			if len(filesInFolder) <= maxNumberOfFilesPerFolder {
				return
			}
			totalLen := len(filesInFolder)
			folderTarget := totalLen / maxNumberOfFilesPerFolder
			for i := 1; i < folderTarget; i++ {
				if i > 1 && len(filesInFolder) >= totalLen {
					panic("files in folder not modified after renames")
				}
				if len(filesInFolder) <= maxNumberOfFilesPerFolder {
					break
				}
				newPath := filepath.Join(path, strconv.Itoa(i))
				println("creating directory " + newPath)
				if err = os.MkdirAll(newPath, 0755); err != nil {
					println(red + err.Error() + reset)
					return
				}
				toMove := len(filesInFolder) / maxNumberOfFilesPerFolder
				for i := 0; i < toMove; i++ {
					target := filepath.Join(path, filesInFolder[i].Name())
					newhome := filepath.Join(newPath, filesInFolder[i].Name())
					println("moving " + target + " to " + newhome)
					if err = os.Rename(target, newhome); err != nil {
						if os.IsNotExist(err) {
							continue
						}
						return
					}
				}
				filesInFolder = filesInFolder[toMove+1:]
			}
		})
		return nil
	}); err != nil {
		if !os.IsNotExist(err) {
			printlnf(red + "failed during rename process: " + err.Error() + reset)
			os.Exit(1)
		}
	}
	return wg
}

func extOf(path string) string {
	return strings.ToUpper(strings.TrimPrefix(filepath.Ext(path), "."))
}

func main() {
	var (
		workersFlag  int
		poolsFlag    int
		helpFlag     bool
		skipDone     bool
		skipTypesStr string
		skipTypes    = make(map[string]bool)
	)

	flag.StringVar(&source, "src", "", "Source directory with files recovered by Photorec")
	flag.StringVar(destination, "dest", "", "Destination directory to write sorted files to")
	flag.IntVar(&maxNumberOfFilesPerFolder, "max-per-dir", 500, "Maximum number of files per directory")
	flag.BoolVar(&splitMonths, "split-months", false, "Split JPEG files not only by year but by month as well")
	flag.BoolVar(&keepFilename, "keep_filename", false, "Keeps the original filenames when copying")
	flag.IntVar(&minEventDeltaDays, "min-event-delta", 4, "Minimum delta in days between two events")
	flag.BoolVar(&dateTimeFilename, "date_time_filename", false, "Sets the filename to the exif date and time if possible")
	flag.BoolVar(&skipDone, "update", false, "Skips any files from source that have symlinks in the destination folder already")
	flag.StringVar(&skipTypesStr, "skip-extensions", "", "Comma separated list of file extensions to skip")
	flag.BoolVar(&verbose, "v", false, "verbose output")
	flag.IntVar(&workersFlag, "workers", 5, "Number of workers to use")
	flag.IntVar(&poolsFlag, "pools", runtime.NumCPU(), "Number of pools to use")

	flag.Usage = func() {
		printArt("H4sIAAAAAAACA12OuxHAMAhDe0/BqB7ARSoducQeTpMEDFxyodETf5EM4pA3GrGITmgBiLPgNpCPXz+w6VmpYY3DZV/IlOkVO7zs/T3sDNH9UFIt0yy3ByyvR6i0AAAA")
		flag.PrintDefaults()
		exNo := 1
		if helpFlag {
			exNo = 0
		}
		os.Exit(exNo)
	}

	flag.Parse()

	if skipTypesStr != "" {
		skipTypesSplit := strings.Split(skipTypesStr, ",")
		for _, ext := range skipTypesSplit {
			skipTypes[strings.ToUpper(ext)] = true
			printlnf("skipping files with extension: " + ext)
		}
	}

	if source == "" || *destination == "" {
		flag.Usage()
	}

	if workersFlag > 0 && poolsFlag > 0 {
		workers, _ = ants.NewMultiPool(workersFlag, poolsFlag, ants.RoundRobin)
	}

	if maxNumberOfFilesPerFolder == 0 {
		printlnf("max-per-dir must be greater than 0, setting it to 500 but flags might be fucked")
		maxNumberOfFilesPerFolder = 500
	}

	var done = make(map[string]struct{})
	doneCount := 0

	_, destStatErr := os.Stat(*destination)
	if errors.Is(destStatErr, os.ErrNotExist) {
		printlnf("destination directory does not exist, creating...")
		if err := os.MkdirAll(*destination, os.ModePerm); err != nil {
			printlnf(red + "FATAL: " + err.Error() + reset)
			os.Exit(1)
		}
	} else if destStatErr != nil {
		printlnf(red + "FATAL: " + destStatErr.Error() + reset)
	}

	if skipDone {
		_ = filepath.Walk(*destination, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() {
				return nil
			}
			if info.Mode()&fs.ModeType != fs.ModeSymlink {
				return nil
			}
			linkTarget, err := os.Readlink(path)
			if err != nil {
				return nil
			}
			done[linkTarget] = struct{}{}
			doneCount++
			return nil
		})
		println("found " + strconv.Itoa(doneCount) + " symlinks already present")
	}

	printlnf("Reading from source '%s', writing to destination '%s' (max %d files per directory, splitting by year %v).\n",
		source, *destination, maxNumberOfFilesPerFolder, splitMonths)

	if keepFilename {
		printlnf("I will keep your filenames as they are")
	} else if dateTimeFilename {
		printlnf("If possible I will rename your files like <Date>_<Time>.jpg - otherwise keep the filenames as they are")
	} else {
		printlnf("I will rename your files like '1.jpg'")
	}

	/*	fileNumber := getNumberOfFilesInFolderRecursively(source)
		totalAmountToCopy := strconv.Itoa(fileNumber)
		printlnf("Files to copy: " + totalAmountToCopy)*/

	var wg = &sync.WaitGroup{}

	setSpinner(spinner.New(spinner.CharSets[43], time.Duration(50)*time.Millisecond))

	getSpinner().Start()

	if err := filepath.Walk(source, func(path string, info os.FileInfo, err error) error {

		if err != nil {
			return err
		}

		if skipDone {
			if _, ok := done[path]; ok {
				return nil
			}
		}

		if info.IsDir() {
			return nil
		}

		if _, ok := skipTypes[extOf(path)]; ok {
			return nil
		}

		wg.Add(1) // workers.Submit will block upon pool exhaustion, so add to the waitgroup early to avoid race
		_ = workers.Submit(func() { processFile(path, wg) })

		return nil

	}); err == nil {
		printlnf("\n\n" + blue + "Waiting for all files to be processed" + reset)
		fmt.Print("\n")
		wg.Wait()
	} else {
		printlnf(red + "Error walking the path " + source + ": " + err.Error() + reset)
		os.Exit(1)
	}

	getSpinner().Stop()

	log("start special file treatment")

	processPath := filepath.Join(*destination, "Processed_Images")
	_ = os.MkdirAll(processPath, 0755)
	postprocessImages(filepath.Join(*destination, "JPG"), processPath)
	postprocessImages(filepath.Join(*destination, "PNG"), processPath)

	log("assure max file per folder number")
	wg = limitFilesPerFolder(*destination, maxNumberOfFilesPerFolder)

	wg.Wait()

	printArt("H4sIAAAAAAACAzWLuw2AQAxD+5vCo1JQUlC9OwlYzpPgBBH5E8WOmWYzmKmeYU7J7OH1ZX28s6sycxRdn/zt1Fagtic6XoQWnC9aAAAA")
}

var magicBufs = sync.Pool{
	New: func() interface{} {
		return make([]byte, 261)
	},
}

func processFile(path string, wg *sync.WaitGroup) {
	defer wg.Done()

	if verbose {
		printlnf("processing: " + path)
	}

	// printlnf("extension for: " + path + " is " + extension)

	extension, destinationDirectory, isImage, isOtherMedia := getDestinationDirectory(path)
	/*if isImage || isWAV {
		printlnf("detected: wav=%t image=%t\n", isWAV, isImage)
	}*/
	if !strings.Contains(destinationDirectory, *destination) {
		panic(fmt.Errorf("destination directory %s is not a subdirectory of %s", destinationDirectory, *destination))
	}
	// printlnf("destination directory: " + destinationDirectory)
	createPath(destinationDirectory)

	fname := filepath.Base(path)
	fileName := getFileName(fname, path, extension, isImage, isOtherMedia)

	destinationFile := filepath.Join(destinationDirectory, fileName)

	if verbose {
		printlnf("symlinking %s to %s\n", path, destinationFile)
	}

	symLink(path, destinationFile)
}

var fastMode bool

func extIsJpg(ext string) bool {
	return strings.ToUpper(ext) == "JPEG" || strings.ToUpper(ext) == "JPG"
}

func extIsOtherMedia(ext string) bool {
	ext = strings.ToUpper(ext)
	return ext == "WAV" || ext == "MP3" || ext == "OGG" || ext == "FLAC" || ext == "ACC" || ext == "MP4"
}

func getDestinationDirectory(path string) (ext, dest string, isJPEG, isOtherMedia bool) {
	// so we can skip exif data for non-media files
	// and use this for data recovery that doesn't append the correct extension
	ext = extOf(path)
	if fastMode && ext != "" {
		return ext,
			filepath.Join(*destination, ext),
			extIsJpg(ext), extIsOtherMedia(ext)
	}

	if fastMode && ext == "" {
		return "UNKNOWN", filepath.Join(*destination, "UNKNOWN"), false, false
	}

	b := magicBufs.Get().([]byte)
	b = b[:0]
	b = b[:261]

	f, err := os.Open(path)
	if err != nil {
		if ext == "" {
			ext = "UNKNOWN"
		}
		return ext, filepath.Join(*destination, ext), extIsJpg(ext), extIsOtherMedia(ext)
	}

	n, rerr := f.Read(b)
	_ = f.Close()
	if rerr == nil && n > 0 {
		kind, kerr := filetype.Match(b)
		if kerr == nil {
			if kind != filetype.Unknown {
				if verbose {
					printlnf("File type: %s. MIME: %s\n", kind.Extension, kind.MIME.Value)
				}
				ext = strings.ToUpper(kind.Extension)
				isOtherMedia = kind.MIME.Subtype == "x-wav" || ext == "WAV"
			}
		}
	}

	if ext == "" {
		ext = "UNKNOWN"
	}

	dest = filepath.Join(*destination, ext)
	isJPEG = filetype.IsImage(b)
	magicBufs.Put(b)
	return ext, dest, isJPEG || extIsJpg(ext), extIsOtherMedia(ext)
}

func nameFromWAV(ogFilename string, f *os.File) (string, error) {
	var err error

	decoder := wav.NewDecoder(f)

	decoder.ReadMetadata()
	if decoder.Err() != nil {
		return ogFilename, decoder.Err()
	}

	if decoder.Metadata == nil {
		return ogFilename, nil
	}

	var newFilename string

	if decoder.Metadata.Title != "" {
		newFilename = decoder.Metadata.Title
	}

	if decoder.Metadata.Artist != "" {
		if newFilename != "" {
			newFilename = decoder.Metadata.Artist + " - " + newFilename
		} else {
			newFilename = decoder.Metadata.Artist + " - " + ogFilename
		}
	}

	if decoder.Metadata.Software != "" {
		if newFilename == "" {
			newFilename = filepath.Join(decoder.Metadata.Software, ogFilename)
		} else {
			newFilename = filepath.Join(decoder.Metadata.Software, newFilename)
		}
	}

	if newFilename == "" {
		err = fmt.Errorf("no metadata found")
	} else {
		printlnf(green+"got WAV metadata for %s%s: %s\n", ogFilename, reset, newFilename)
	}

	return newFilename, err
}

func nameFromOtherMedia(ogFilename string, f *os.File) (string, error) {
	meta, err := tag.ReadFrom(f)
	if err != nil {
		return ogFilename, err
	}

	var aka = ""

	newFilename := cleanString(meta.Title())

	if artist := cleanString(meta.Artist()); artist != "" {
		newFilename = artist + "_" + newFilename
	}

	if newFilename != "" {
		aka = " AKA " + meta.Title()
	}

	printlnf(
		green +
			"got " + string(meta.Format()) +
			" metadata for " + string(meta.FileType()) +
			" file " + ogFilename + aka +
			reset,
	)

	for k, v := range meta.Raw() {
		if len(fmt.Sprintf("%v", k)) == 0 || v == nil {
			continue
		}
		printlnf("\t%s%s%s: (%T) %v\n", blue, k, reset, v, cleanString(fmt.Sprintf("%v", v)))
	}

	if newFilename == "" {
		for _, s := range []string{
			meta.Title(), meta.Comment(),
		} {
			if s != "" {
				newFilename = s
				break
			}
		}
	}

	if newFilename == "" {
		return ogFilename, fmt.Errorf("no title found in " + string(meta.Format()) + " metadata")
	}

	return newFilename, nil
}

func getMediaNameFromMetadata(path string) (string, error) {
	_, ogFilename := filepath.Split(path)

	f, err := os.Open(path)
	if err != nil {
		return ogFilename, fmt.Errorf("couldn't open %s: %s", path, err.Error())
	}
	defer func() {
		_ = f.Close()
	}()

	if e := extOf(path); e == "WAV" {
		return nameFromWAV(ogFilename, f)
	}

	return nameFromOtherMedia(ogFilename, f)
}

func cleanString(fname string, extension ...string) string {
	if len(fname) > maxFilenameLength {
		fname = fname[:maxFilenameLength]
		if len(extension) == 1 && extension[0] != "" && extension[0] != "." {
			ext := extension[0]
			if !strings.HasPrefix(ext, ".") {
				ext = "." + ext
			}
			fname += ext
		}
	}
	u8, _ := unicode.UTF8.NewEncoder().String(fname)
	if u8 != "" {
		fname = u8
	}
	fname = strings.Trim(strings.TrimSpace(fname), "'")
	return fname
}

func getFileName(fname string, sourcePath, extension string, isImage, isOtherMedia bool) string {
	fname = cleanString(filepath.Base(fname))
	if (keepFilename || fastMode || (!isImage && !isOtherMedia) || !dateTimeFilename) && fname != "" {
		return fname
	}

	if dateTimeFilename && !fastMode && !keepFilename && isImage {
		if verbose {
			printlnf("getting filename for: " + sourcePath)
		}
		fname2 := getImageNameFromMetadata(sourcePath, extension, fname)
		if fname2 != fname {
			fname = cleanString(fname2)
			if verbose {
				printlnf("got filename: " + fname)
			}
		}
	}

	if dateTimeFilename && !fastMode && !keepFilename && isOtherMedia {
		newFilename, err := getMediaNameFromMetadata(sourcePath)
		if err == nil && newFilename != fname {
			fname = cleanString(newFilename)
			if verbose {
				printlnf("got filename: " + fname)
			}
		}

	}

	if !isImage && !isOtherMedia {
		return fname
	}

	if fname == "" {
		fstat, err := os.Stat(sourcePath)
		if err == nil {
			fname = fstat.ModTime().Format("2006-01-02_15-04-05")
		} else {
			fname = "unknown"
		}
		fname = fname + "." + strings.ToLower(extension)
	}

	index := 0
	for fileExists(filepath.Join(*destination, extension, fname)) {
		index++
		fbase := strings.TrimSuffix(fname, filepath.Ext(fname))
		if strings.Contains(fbase, "_") {
			fbase = strings.Split(fbase, "_")[0]
		}
		fname = fmt.Sprintf("%s_%d%s", fbase, index, filepath.Ext(fname))
	}

	return fname
}

func getImageNameFromMetadata(sourcePath, extension, fallbackName string) string {

	rawExifData, err := getRawExif(sourcePath)

	if err != nil {
		if verbose {
			printlnf(sourcePath + ": " + err.Error())
		}
		return fallbackName
	}

	creationTime, err := getMinimumCreationTime(rawExifData)
	if creationTime == nil || err != nil {
		if err == nil {
			err = fmt.Errorf("%s: nil creation time", sourcePath)
		}
		if verbose {
			printlnf(err.Error())
		}
		return fallbackName
	}

	creationTimeStr := creationTime.Format("2006-01-02_15-04-05")
	if creationTimeStr == "" {
		panic("bad time format")
	}

	return creationTimeStr + "." + strings.ToLower(extension)
}
