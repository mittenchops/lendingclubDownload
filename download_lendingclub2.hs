#!/usr/bin/env stack
{-# LANGUAGE OverloadedStrings, LambdaCase  #-}
import Control.Exception
import Control.Monad.Fix
import Control.Retry
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as B
import Data.List (sort)
import Data.Monoid ((<>))
import Data.String.Utils
import Data.Time
import Network.HTTP.Client
import Network.HTTP.Client.TLS   (tlsManagerSettings)
import System.Directory
import System.Exit
import System.FilePath.Posix
import System.IO
import System.IO.Posix.MMap (unsafeMMapFile)
import Text.Regex.Posix
import System.Environment
--

--
import System.IO                 (stdout)
import System.Log.Formatter      (simpleLogFormatter)
import System.Log.Handler        (setFormatter)
import System.Log.Handler.Simple (streamHandler)
import System.Log.Logger         (Priority (DEBUG, INFO, WARNING, ERROR), debugM, infoM, warningM,
                                  errorM, rootLoggerName,setHandlers, setLevel, updateGlobalLogger)
-- ref http://fakedrake.github.io/streaming-data-through-http-with-haskell.html

initLogging :: Priority -> IO ()
initLogging level = do
    stdOutHandler <- streamHandler stdout level >>= \lh -> return $
            setFormatter lh (simpleLogFormatter "[$prio:$loggername:$time] $msg")
    updateGlobalLogger rootLoggerName (setLevel level . setHandlers [stdOutHandler])

ppi :: String  -> IO ()
ppi msg = infoM "" msg

ppd :: String  -> IO ()
ppd msg = debugM "" msg


cDOWNLOADDIR = "/tmp/"
cDATADIR = "/mydata/Data/LendingClub/" -- mydata
cURL = "https://resources.lendingclub.com/SecondaryMarketAllNotes.csv"
cFNAME = "SecondaryMarketAllNotes.csv.part"
cBACKOFF_FACTOR = 1000000

-- λ> sha256 $ B.pack "meow"
-- 404cdd7bc109c432f8cc2443b45bcfe95980f5107215c645236e577929ac3e52

getLatestFname :: FilePath -> IO String
getLatestFname dirname = do
  cwd <- makeAbsolute dirname
  ls <- getDirectoryContents cwd
  let csvs = sort $ filter (\x -> x =~ ("csv$" :: String)) ls
  recent <- makeAbsolute $ last csvs
  ppi $ "Most recent filename is: " ++ recent
  return recent

verifyFirstAndLast :: FilePath -> IO Bool
verifyFirstAndLast recent = do
  bs <- unsafeMMapFile recent
  let (firstLine, _) = BS.break (== '\n') bs
      (_, lastLine) = BS.breakEnd (== '\n') (BS.init bs)
  ppd "Sample output: "
  ppd $ "First line: " ++ BS.unpack firstLine
  ppd $ "Last line: " ++ BS.unpack lastLine
  let fl = glen firstLine
  let trth = (fl == glen lastLine)
  ppd $ "Number of fields: " ++ show fl
  if fl /= 21 then die "NOT ENOUGH FIELDS" else return trth
  where
    glen = (\x -> length $ split "," (BS.unpack x))

download :: String -> String -> IO ()
download url tmpname = do
  man <- newManager tlsManagerSettings
  req' <- parseUrlThrow url
  ppi url
  let req = req' { responseTimeout = Just 1000000}
  bracket (openFile tmpname WriteMode) (hClose) (\fh -> do
    withResponse req man $ \res -> do
    ppd $ "RESPONSE HEADERS: " ++ (show . responseHeaders) res
    ppd "Got a response, now streaming..."
    ppd =<< makeTime
    fix $ \loop -> do
      bytes <- brRead $ responseBody res
      if BS.null bytes then ppd "Download complete!" else do
        BS.hPutStr fh bytes 
        loop)
  ppd =<< makeTime

fTime :: String -> IO String
fTime fmt = (formatTime defaultTimeLocale fmt) <$> getZonedTime

makeTime :: IO String
makeTime = fTime "%Y%m%d%H%M%S"

makeTstamp :: IO String
makeTstamp = fTime "%Y%m%d_%H%M"

makeNewName :: String -> String -> IO String
makeNewName datadir tmpname = do
  tf <- doesDirectoryExist datadir
  if tf then do
    tstamp <- makeTstamp
    let oname = replace ".csv.part" ("_" ++ tstamp ++ ".csv") tmpname
    let newname = datadir </> oname  
    return newname
  else
    die "datadir does not exist"

makeTmpName :: String -> String -> IO String
makeTmpName dir filename = do
  tf <- doesDirectoryExist dir
  if tf then do
    let tmpname = dir </> filename
    return tmpname
  else
    die "tmp directory does not exist"

mvOutFile :: FilePath -> String -> IO ()
mvOutFile tmpname newname = do
  fileExist <- doesFileExist tmpname
  direxists <- doesDirectoryExist $ takeDirectory newname
  if (fileExist && direxists)
    then do
      ppd "File exists"
      copyFileWithMetadata tmpname newname
      ppd $ "Copied to " ++ newname
    else do
      ppi "Oh no!  No file!"

getFileSize :: FilePath -> IO Integer
getFileSize fpath = do
  sz <- withFile fpath ReadMode hFileSize
  return sz

sizeChecksOut :: String -> IO Bool
sizeChecksOut fpath = do 
  sz <- getFileSize fpath
  let mbs = (fromIntegral sz) / 1e6
  ppi $ "FILESIZE: " ++ show mbs ++ " MB"
  let tf = if (mbs > 70) then True else False
  return tf

dlfile :: IO ()
dlfile = do
  datadirexists <- doesDirectoryExist cDATADIR
  tmpdirexists <- doesDirectoryExist cDOWNLOADDIR
  if not (datadirexists && tmpdirexists) then
    die "Can't write to necessary directories"
  else
    ppd "dirs are writable"  
  tmpname <- makeTmpName cDOWNLOADDIR cFNAME
  newname <- makeNewName cDATADIR cFNAME
  download cURL tmpname
  safe <- verifyFirstAndLast tmpname
  sized <- sizeChecksOut tmpname
  if (safe && sized)
    then do
      ppi "Fidelity Checks.  Copying file to timestamped..."
      ppd $ "this is tmpname: " ++ tmpname
      ppd $ "this is newname: " ++ newname
      mvOutFile tmpname newname
      ppi $ "Success."
    else do
      tf <- (doesFileExist tmpname)
      if tf
        then do
          removeFile tmpname
          die "Fidelity Fails"
        else 
          die "Could not remove file"

-- http://stackoverflow.com/questions/41913695/haskell-simple-getargs-type-error
parseArgs :: [String] -> (Priority, Int, Int)
parseArgs [a,b,c] = (read a, cBACKOFF, read c)
  where cBACKOFF = round $ (read (b) :: Double ) * (fromIntegral cBACKOFF_FACTOR)
parseArgs _     = (DEBUG, round (0.5 * fromIntegral cBACKOFF_FACTOR), 5)

argPrint :: (Priority, Int, Int) -> IO ()
argPrint (cDEBUGLEVEL, cBACKOFF, cRETRIES) = do
  ppi $ "DEBUG LEVEL IS: " ++ show cDEBUGLEVEL
  ppi $ "EXPONENTIAL BACKOFF: " ++ show cBACKOFF ++ "\t IN SECONDS: " ++ show (fromIntegral cBACKOFF / fromIntegral cBACKOFF_FACTOR)
  ppi $ "NUMBER OF RETRIES: " ++  show cRETRIES

main :: IO ()
main = do
  -- arg 1 = DEBUG | INFO | WARNING
  -- arg 2 = seconds to init 0.5s -- the function will convert this to microseconds
  -- arg 3 = retry count
  args <- getArgs
  let parsedArgs = parseArgs args
  argPrint parsedArgs
  let (cDEBUGLEVEL, cBACKOFF, cRETRIES) = parsedArgs
  initLogging cDEBUGLEVEL
  let policy = (exponentialBackoff cBACKOFF <> limitRetries cRETRIES)
  recoverAll policy (const dlfile)
