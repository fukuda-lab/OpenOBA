#!/usr/bin/env bash
set -e
# Use the Unbranded build that corresponds to a specific Firefox version
# To upgrade:
#    1. Go to: https://hg.mozilla.org/releases/mozilla-beta/tags. ----> For some reason, BannerClick was using a mozilla-beta verion of Firefox (https://github.com/bannerclick/bannerclick/commit/40726f3a000df321ff7ae0720d35117525a7a685#diff-826dd6ac7f71ef6dc666338f68fe30ae7d89caaba3e66cf53366ea0769dafdda)
#    1. Go to: https://hg.mozilla.org/releases/mozilla-release/tags. -> This is the correct link to get the Firefox version
#    2. Find the commit hash for the Firefox release version you'd like to upgrade to.
#    3. Update the `TAG` variable below to that hash.

# Note this script is **destructive** and will
# remove the existing Firefox in the OpenWPM directory

# TAG='a486bbf619936d4f8516c853ea6ffad2d576e2a3' # FIREFOX_108_0_2_RELEASE

case "$(uname -s)" in
Darwin)
  echo 'Installing for Mac OSX'
  OS='macosx'
  TARGET_SUFFIX='.dmg'
  ;;
Linux)
  echo 'Installing for Linux'
  OS='linux'
  TARGET_SUFFIX='.tar.bz2'
  ;;
*)
  echo 'Your OS is not supported. Aborting'
  exit 1
  ;;
esac

# We are not using the unbranded build from Mozilla CI because the tested version of Firefox (FIREFOX_108_0_2_RELEASE) is not available for download anymore.
# We are using the Firefox version included in the repository (scripts/firefox_108_0_2) instead.
# UNBRANDED_RELEASE_BUILD="https://firefox-ci-tc.services.mozilla.com/api/index/v1/task/gecko.v2.mozilla-release.revision.${TAG}.firefox.${OS}64-add-on-devel/artifacts/public/build/target${TARGET_SUFFIX}"
# wget "$UNBRANDED_RELEASE_BUILD"

case "$(uname -s)" in
Darwin)
  rm -rf Nightly.app || true
  # hdiutil attach -nobrowse -mountpoint /Volumes/firefox-tmp target.dmg ----> This is the original line from OpenWPM and Bannerclick
  hdiutil attach -nobrowse -mountpoint /Volumes/firefox-tmp scripts/firefox_108_0_2/macosx_firefox_ver_108_0_2.dmg
  cp -r /Volumes/firefox-tmp/Nightly.app .
  hdiutil detach /Volumes/firefox-tmp
  # rm target.dmg
  ;;
Linux)
  # tar jxf target.tar.bz2 ----> This is the original line from OpenWPM and Bannerclick
  tar jxf scripts/firefox_108_0_2/linux_firefox_ver_108_0_2.tar.bz2
  rm -rf firefox-bin
  mv firefox firefox-bin
  # rm target.tar.bz2
  ;;
esac

echo 'Firefox succesfully installed'
