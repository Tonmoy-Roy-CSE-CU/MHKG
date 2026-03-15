/* MHKG Chunked ABox Uploader
 * Placed in the Dash assets/ folder so it loads automatically on every page.
 * Creates a hidden <input type="file">, hooks the Browse button and drop zone,
 * then streams the file to Flask in 4 MB chunks.
 */
(function () {
  "use strict";

  var CHUNK = 4 * 1024 * 1024; // 4 MB per request

  /* ── wait until the Dash layout has rendered ── */
  function init() {
    var browseBtn = document.getElementById("abox-browse-btn");
    var dropZone  = document.getElementById("abox-drop-zone");
    var wrap      = document.getElementById("abox-file-wrap");

    if (!browseBtn || !dropZone || !wrap) {
      setTimeout(init, 300);
      return;
    }
    if (wrap._mhkg_ready) return;
    wrap._mhkg_ready = true;

    /* ── create the hidden native file input ── */
    var fileInput = document.createElement("input");
    fileInput.type    = "file";
    fileInput.accept  = ".ttl,.n3,.nt";
    fileInput.id      = "abox-real-input";
    fileInput.style.cssText = "position:absolute;width:100%;height:100%;top:0;left:0;" +
                              "opacity:0;cursor:pointer;z-index:10;";
    wrap.appendChild(fileInput);

    /* ── Browse button click → open file dialog ── */
    browseBtn.addEventListener("click", function (e) {
      e.preventDefault();
      fileInput.value = "";      // reset so same file can be re-selected
      fileInput.click();
    });

    /* ── Drag & drop onto the card ── */
    dropZone.addEventListener("dragover",  function (e) { e.preventDefault(); dropZone.style.borderColor="#28a745"; });
    dropZone.addEventListener("dragleave", function ()  { dropZone.style.borderColor=""; });
    dropZone.addEventListener("drop", function (e) {
      e.preventDefault();
      dropZone.style.borderColor = "";
      var file = e.dataTransfer.files[0];
      if (file) startUpload(file);
    });

    /* ── File chosen via dialog ── */
    fileInput.addEventListener("change", function () {
      if (fileInput.files[0]) startUpload(fileInput.files[0]);
    });
  }

  /* ── helpers ── */
  function setStatus(msg) {
    var el = document.getElementById("upload-abox-status");
    if (el) el.innerText = msg;
  }

  function triggerPoll() {
    /* Write to the hidden dcc.Input so Dash's clientside callback enables
       the progress interval */
    var el = document.getElementById("abox-upload-trigger");
    if (!el) return;
    el.value = String(Date.now());
    el.dispatchEvent(new Event("input",  { bubbles: true }));
    el.dispatchEvent(new Event("change", { bubbles: true }));
  }

  /* ── main upload flow ── */
  function startUpload(file) {
    var totalMB = (file.size / 1048576).toFixed(1);
    setStatus("Initialising upload of " + totalMB + " MB…");

    fetch("/upload-abox-init", {
      method:  "POST",
      headers: { "Content-Type": "application/json" },
      body:    JSON.stringify({ filename: file.name, size: file.size }),
    })
    .then(function (r) { return r.json(); })
    .then(function (data) {
      if (data.error) { setStatus("Init error: " + data.error); return; }
      triggerPoll();
      sendChunks(file, data.tmp, 0);
    })
    .catch(function (e) { setStatus("Init failed: " + e); });
  }

  function sendChunks(file, tmpPath, offset) {
    if (offset >= file.size) {
      /* All chunks sent → finalise */
      fetch("/upload-abox-finalise", {
        method:  "POST",
        headers: { "Content-Type": "application/json" },
        body:    JSON.stringify({ tmp: tmpPath, filename: file.name }),
      })
      .then(function (r) { return r.json(); })
      .then(function () { setStatus("Upload complete — parsing on server…"); })
      .catch(function (e) { setStatus("Finalise error: " + e); });
      return;
    }

    var slice = file.slice(offset, offset + CHUNK);
    var sent  = Math.min(offset + CHUNK, file.size);
    var pct   = Math.round(sent / file.size * 100);

    fetch("/upload-abox-chunk", {
      method:  "POST",
      headers: {
        "X-Tmp-Path":   tmpPath,
        "Content-Type": "application/octet-stream",
      },
      body: slice,
    })
    .then(function (r) { return r.json(); })
    .then(function () {
      setStatus(
        "Uploading… " + pct + "% (" +
        (sent / 1048576).toFixed(0) + " / " +
        (file.size / 1048576).toFixed(0) + " MB)"
      );
      sendChunks(file, tmpPath, offset + CHUNK);
    })
    .catch(function (e) { setStatus("Chunk error at " + offset + ": " + e); });
  }

  /* ── boot ── */
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  /* Re-run after Dash re-renders (layout updates reset the DOM) */
  var _observer = new MutationObserver(function () { init(); });
  _observer.observe(document.body, { childList: true, subtree: true });

})();
